package provisioning

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"nexus-gateway/internal/pointlist"
)

// Client is the interface for the Building OS provisioning API.
// Develop against MockClient; swap for HTTPClient when gutp-building-os-oss #224 lands.
type Client interface {
	// VersionToken returns a cheap opaque token; change means the snapshot must be re-fetched.
	VersionToken(ctx context.Context) (string, error)
	// Snapshot returns the full authoritative Point List.
	Snapshot(ctx context.Context) ([]pointlist.Entry, error)
}

// HTTPClient implements Client against the real provisioning HTTP API.
type HTTPClient struct {
	baseURL string
	http    *http.Client
}

// NewHTTPClient creates a provisioning client for the given base URL.
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{baseURL: baseURL, http: &http.Client{}}
}

func (c *HTTPClient) VersionToken(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/version", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("provisioning: version status %d", resp.StatusCode)
	}
	var v struct{ Version string `json:"version"` }
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", err
	}
	return v.Version, nil
}

func (c *HTTPClient) Snapshot(ctx context.Context) ([]pointlist.Entry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/snapshot", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provisioning: snapshot status %d", resp.StatusCode)
	}
	var entries []pointlist.Entry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, err
	}
	return entries, nil
}
