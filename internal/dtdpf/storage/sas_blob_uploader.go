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
	"strconv"
	"strings"
	"time"

	"nexus-gateway/internal/retry"
)

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
	u := &SASBlobUploader{
		baseURL:     parsed,
		httpClient:  http.DefaultClient,
		timeout:     30 * time.Second,
		maxAttempts: 3,
		backoff:     retry.Backoff{Min: 200 * time.Millisecond, Max: 5 * time.Second, Factor: 2},
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
	return u, nil
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
	return u.baseURL.JoinPath(objectName).String()
}

func (u *SASBlobUploader) putOnce(ctx context.Context, objectURL string, body []byte) error {
	attemptCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPut, objectURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("x-ms-blob-type", "BlockBlob")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", strconv.Itoa(len(body)))

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return &retryableError{err: err}
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
