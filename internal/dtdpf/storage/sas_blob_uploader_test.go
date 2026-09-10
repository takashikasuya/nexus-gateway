// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package storage_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/dtdpf/storage"
	"nexus-gateway/internal/retry"
)

// requestRecord captures one observed PUT request for assertions.
type requestRecord struct {
	method, path, query string
	blobType, ctype     string
	body                []byte
}

func newFakeServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, seen *requestRecord)) (*httptest.Server, *[]requestRecord) {
	t.Helper()
	var mu sync.Mutex
	var records []requestRecord
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		rec := requestRecord{
			method: r.Method, path: r.URL.Path, query: r.URL.RawQuery,
			blobType: r.Header.Get("x-ms-blob-type"), ctype: r.Header.Get("Content-Type"),
			body: body,
		}
		// A client-side per-attempt timeout can leave the previous request's
		// handler goroutine still running (e.g. blocked in time.Sleep) while a
		// retried request's handler runs concurrently; guard shared state.
		mu.Lock()
		records = append(records, rec)
		mu.Unlock()
		handler(w, r, &rec)
	}))
	t.Cleanup(server.Close)
	return server, &records
}

func TestSASBlobUploader_PutSendsExpectedRequest(t *testing.T) {
	server, records := newFakeServer(t, func(w http.ResponseWriter, r *http.Request, seen *requestRecord) {
		w.WriteHeader(http.StatusCreated)
	})

	uploader, err := storage.NewSASBlobUploader(server.URL + "/container?sv=2021&sig=secret")
	require.NoError(t, err)

	require.NoError(t, uploader.Put(context.Background(), "43217568.json", []byte(`{"a":1}`)))

	require.Len(t, *records, 1)
	got := (*records)[0]
	assert.Equal(t, http.MethodPut, got.method)
	assert.Equal(t, "/container/43217568.json", got.path, "object name is appended to the container path")
	assert.Equal(t, "sv=2021&sig=secret", got.query, "the SAS query string is preserved unchanged")
	assert.Equal(t, "BlockBlob", got.blobType)
	assert.Equal(t, "application/json", got.ctype)
	assert.Equal(t, `{"a":1}`, string(got.body))
}

func TestSASBlobUploader_RetriesTransient5xxThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	server, _ := newFakeServer(t, func(w http.ResponseWriter, r *http.Request, seen *requestRecord) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	uploader, err := storage.NewSASBlobUploader(server.URL+"/container?sig=x",
		storage.WithMaxAttempts(3),
		storage.WithBackoff(retry.Backoff{Min: time.Millisecond, Max: time.Millisecond, Factor: 1}),
	)
	require.NoError(t, err)

	require.NoError(t, uploader.Put(context.Background(), "obj.json", []byte("x")))
	assert.Equal(t, int32(2), calls.Load(), "must retry once after the transient 500 before succeeding")
}

func TestSASBlobUploader_GivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	server, _ := newFakeServer(t, func(w http.ResponseWriter, r *http.Request, seen *requestRecord) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})

	uploader, err := storage.NewSASBlobUploader(server.URL+"/container?sig=x",
		storage.WithMaxAttempts(3),
		storage.WithBackoff(retry.Backoff{Min: time.Millisecond, Max: time.Millisecond, Factor: 1}),
	)
	require.NoError(t, err)

	err = uploader.Put(context.Background(), "obj.json", []byte("x"))
	require.Error(t, err)
	assert.Equal(t, int32(3), calls.Load(), "must stop after exactly maxAttempts")
}

func TestSASBlobUploader_DoesNotRetryOn4xx(t *testing.T) {
	var calls atomic.Int32
	server, _ := newFakeServer(t, func(w http.ResponseWriter, r *http.Request, seen *requestRecord) {
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
	})

	uploader, err := storage.NewSASBlobUploader(server.URL+"/container?sig=x", storage.WithMaxAttempts(3))
	require.NoError(t, err)

	err = uploader.Put(context.Background(), "obj.json", []byte("x"))
	require.Error(t, err)
	assert.Equal(t, int32(1), calls.Load(), "a 4xx response is a misconfiguration, not a transient failure — fail fast")
}

func TestSASBlobUploader_PerAttemptTimeoutIsRetryable(t *testing.T) {
	var calls atomic.Int32
	server, _ := newFakeServer(t, func(w http.ResponseWriter, r *http.Request, seen *requestRecord) {
		if calls.Add(1) == 1 {
			time.Sleep(50 * time.Millisecond) // exceeds the configured per-attempt timeout
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	uploader, err := storage.NewSASBlobUploader(server.URL+"/container?sig=x",
		storage.WithTimeout(5*time.Millisecond),
		storage.WithMaxAttempts(2),
		storage.WithBackoff(retry.Backoff{Min: time.Millisecond, Max: time.Millisecond, Factor: 1}),
	)
	require.NoError(t, err)

	require.NoError(t, uploader.Put(context.Background(), "obj.json", []byte("x")))
	assert.Equal(t, int32(2), calls.Load(), "a per-attempt timeout must be retried, not treated as final")
}

func TestSASBlobUploader_ObjectNameIsPathEscaped(t *testing.T) {
	server, records := newFakeServer(t, func(w http.ResponseWriter, r *http.Request, seen *requestRecord) {
		w.WriteHeader(http.StatusCreated)
	})

	uploader, err := storage.NewSASBlobUploader(server.URL + "/container?sig=x")
	require.NoError(t, err)

	require.NoError(t, uploader.Put(context.Background(), "a b#c.json", []byte("x")))

	require.Len(t, *records, 1)
	assert.Equal(t, "/container/a b#c.json", (*records)[0].path,
		"the server-observed decoded path must match the literal object name")
}

func TestNewSASBlobUploader_RejectsNonPositiveMaxAttempts(t *testing.T) {
	_, err := storage.NewSASBlobUploader("https://example.blob.core.windows.net/container?sig=x", storage.WithMaxAttempts(0))
	require.Error(t, err)
}

func TestNewSASBlobUploader_RejectsNilHTTPClient(t *testing.T) {
	_, err := storage.NewSASBlobUploader("https://example.blob.core.windows.net/container?sig=x", storage.WithHTTPClient(nil))
	require.Error(t, err)
}

func TestNewSASBlobUploader_RejectsNonPositiveTimeout(t *testing.T) {
	_, err := storage.NewSASBlobUploader("https://example.blob.core.windows.net/container?sig=x", storage.WithTimeout(0))
	require.Error(t, err)
}

func TestNewSASBlobUploader_ParseErrorDoesNotLeakSASSignature(t *testing.T) {
	// A control character makes url.Parse fail; the SAS signature must not
	// appear in the returned error text.
	_, err := storage.NewSASBlobUploader("https://example.blob.core.windows.net/container?sig=super-secret-value\x7f")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "super-secret-value")
}
