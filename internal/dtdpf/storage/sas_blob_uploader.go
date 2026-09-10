// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"nexus-gateway/internal/retry"
)

// defaultAPIVersion is the Azure Storage REST API version sent as
// x-ms-version on every request. Azure Blob Storage's Put Blob operation
// rejects requests missing this header even when the SAS token is valid.
const defaultAPIVersion = "2023-11-03"

// SASBlobUploader implements Uploader with a direct PUT to a container SAS
// URL (DTDPF contract ④ Binding B). It retries transient (network-level or
// 5xx) failures with truncated exponential backoff; 4xx responses are
// treated as non-retryable misconfiguration and fail fast.
type SASBlobUploader struct {
	baseURL     *url.URL
	httpClient  *http.Client
	timeout     time.Duration
	maxAttempts int
	backoff     retry.Backoff
	apiVersion  string
}

// Option configures a SASBlobUploader.
type Option func(*SASBlobUploader)

// WithHTTPClient overrides the HTTP client (default http.DefaultClient); used
// by tests to point at a fake server without touching the default transport.
func WithHTTPClient(c *http.Client) Option {
	return func(u *SASBlobUploader) { u.httpClient = c }
}

// WithTimeout overrides the per-attempt timeout (default 30s).
func WithTimeout(d time.Duration) Option {
	return func(u *SASBlobUploader) { u.timeout = d }
}

// WithMaxAttempts overrides the maximum number of attempts (default 3).
func WithMaxAttempts(n int) Option {
	return func(u *SASBlobUploader) { u.maxAttempts = n }
}

// WithBackoff overrides the retry backoff policy (default 200ms-5s, factor 2).
func WithBackoff(b retry.Backoff) Option {
	return func(u *SASBlobUploader) { u.backoff = b }
}

// WithAPIVersion overrides the Azure Storage REST API version sent as
// x-ms-version (default defaultAPIVersion).
func WithAPIVersion(v string) Option {
	return func(u *SASBlobUploader) { u.apiVersion = v }
}

// NewSASBlobUploader builds an Uploader that PUTs directly to sasURL, a
// container-level (or account-level) SAS URL. Object names are appended to
// the URL path; the SAS query string is preserved unchanged.
func NewSASBlobUploader(sasURL string, opts ...Option) (*SASBlobUploader, error) {
	if strings.TrimSpace(sasURL) == "" {
		return nil, errors.New("DTDPF upload SAS URL is required")
	}
	parsed, err := url.Parse(sasURL)
	if err != nil {
		// Unwrap to the underlying reason only: a *url.Error's Error() method
		// embeds the raw URL (including the SAS signature), which must never
		// be logged.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			return nil, fmt.Errorf("parse DTDPF upload SAS URL: %w", urlErr.Err)
		}
		return nil, errors.New("parse DTDPF upload SAS URL: invalid URL")
	}
	if !parsed.IsAbs() || parsed.Host == "" {
		return nil, errors.New("DTDPF upload SAS URL must be an absolute URL with a scheme and host")
	}
	u := &SASBlobUploader{
		baseURL:     parsed,
		httpClient:  http.DefaultClient,
		timeout:     30 * time.Second,
		maxAttempts: 3,
		backoff:     retry.Backoff{Min: 200 * time.Millisecond, Max: 5 * time.Second, Factor: 2},
		apiVersion:  defaultAPIVersion,
	}
	for _, opt := range opts {
		opt(u)
	}
	if u.maxAttempts < 1 {
		return nil, fmt.Errorf("DTDPF upload max attempts must be at least 1, got %d", u.maxAttempts)
	}
	if u.httpClient == nil {
		return nil, errors.New("DTDPF upload HTTP client must not be nil")
	}
	if u.timeout <= 0 {
		return nil, fmt.Errorf("DTDPF upload timeout must be positive, got %s", u.timeout)
	}
	if strings.TrimSpace(u.apiVersion) == "" {
		return nil, errors.New("DTDPF upload API version must not be empty")
	}
	return u, nil
}

// NewSASBlobUploaderFromEnv builds an Uploader whose SAS URL is read from the
// named environment variable only (e.g. "DTDPF_UPLOAD_SAS_URL") — never from
// a CLI flag — so it cannot leak via process listings or shell history,
// matching the precedent set for DTDPF_EVENTHUB_CONNECTION_STRING.
func NewSASBlobUploaderFromEnv(envVar string, opts ...Option) (*SASBlobUploader, error) {
	sasURL := os.Getenv(envVar)
	if sasURL == "" {
		return nil, fmt.Errorf("environment variable %s is required for the DTDPF upload SAS URL", envVar)
	}
	return NewSASBlobUploader(sasURL, opts...)
}

// Put uploads body as a block blob named objectName, retrying transient
// failures up to maxAttempts times.
func (u *SASBlobUploader) Put(ctx context.Context, objectName string, body []byte) error {
	objectURL := u.objectURL(objectName)
	backoff := u.backoff // per-call copy: independent progression each Put

	var lastErr error
	for attempt := 1; attempt <= u.maxAttempts; attempt++ {
		err := u.putOnce(ctx, objectURL, body)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return fmt.Errorf("upload %s: %w", objectName, err)
		}
		if attempt == u.maxAttempts {
			break
		}
		if waitErr := backoff.Wait(ctx); waitErr != nil {
			return fmt.Errorf("upload %s: %w", objectName, waitErr)
		}
	}
	return fmt.Errorf("upload %s: giving up after %d attempts: %w", objectName, u.maxAttempts, lastErr)
}

func (u *SASBlobUploader) objectURL(objectName string) string {
	// PathEscape treats objectName as one opaque path segment, percent-encoding
	// '/' (and other reserved characters) so it cannot be misread as a path
	// separator and escape the configured container prefix.
	escapedName := url.PathEscape(objectName)
	ref := *u.baseURL
	basePath := strings.TrimSuffix(ref.Path, "/")
	baseEscapedPath := strings.TrimSuffix(ref.EscapedPath(), "/") // read before mutating ref.Path below
	ref.Path = basePath + "/" + objectName
	ref.RawPath = baseEscapedPath + "/" + escapedName
	return ref.String()
}

func (u *SASBlobUploader) putOnce(ctx context.Context, objectURL string, body []byte) error {
	attemptCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPut, objectURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("x-ms-blob-type", "BlockBlob")
	req.Header.Set("x-ms-version", u.apiVersion)
	req.Header.Set("x-ms-date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", strconv.Itoa(len(body)))

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return &retryableError{err: redactSASQuery(err)}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 500:
		return &retryableError{err: fmt.Errorf("unexpected status %d", resp.StatusCode)}
	default:
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

// retryableError marks a putOnce failure as transient (network error or 5xx),
// distinguishing it from a non-retryable 4xx misconfiguration.
type retryableError struct{ err error }

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

func isRetryable(err error) bool {
	var re *retryableError
	return errors.As(err, &re)
}

// redactSASQuery strips the query string (which carries the SAS signature)
// out of a *url.Error's embedded URL before it can reach a log line, while
// preserving the rest of the error chain via %w.
func redactSASQuery(err error) error {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	redactedURL := urlErr.URL
	if i := strings.IndexByte(redactedURL, '?'); i >= 0 {
		redactedURL = redactedURL[:i] + "?<redacted>"
	}
	return fmt.Errorf("%s %s: %w", urlErr.Op, redactedURL, urlErr.Err)
}
