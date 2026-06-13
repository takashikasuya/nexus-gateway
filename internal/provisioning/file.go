package provisioning

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"nexus-gateway/internal/pointlist"
)

// FileClient is a file-backed provisioning.Client: it serves a Point List
// snapshot derived from a local CSV (SBCO point-list format) or JSON file.
//
// It is interface-compatible with the real Building OS provisioning API
// (HTTPClient, gutp-building-os-oss #224), so the gateway can sync from a file
// during dev/E2E and switch to the authoritative API without code changes. Per
// ADR-0003 a provisioning snapshot always overrides any local bootstrap.
type FileClient struct {
	path        string
	connectorID string // used to stamp BACnet CSV entries
}

// NewFileClient serves the Point List from path. A .csv file is parsed via
// LoadCSV (BACnet native-address projection); any other extension is parsed as
// a JSON array of pointlist.Entry.
func NewFileClient(path, connectorID string) *FileClient {
	return &FileClient{path: path, connectorID: connectorID}
}

// VersionToken returns a content hash of the backing file; it changes whenever
// the file content changes, triggering a re-fetch in the sync loop.
func (c *FileClient) VersionToken(_ context.Context) (string, error) {
	data, err := os.ReadFile(c.path)
	if err != nil {
		return "", fmt.Errorf("provisioning: read %q: %w", c.path, err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

// Snapshot parses the backing file into Point List entries.
func (c *FileClient) Snapshot(_ context.Context) ([]pointlist.Entry, error) {
	data, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("provisioning: read %q: %w", c.path, err)
	}
	if strings.HasSuffix(strings.ToLower(c.path), ".csv") {
		return pointlist.LoadCSV(strings.NewReader(string(data)), c.connectorID)
	}
	var entries []pointlist.Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("provisioning: parse %q as JSON: %w", c.path, err)
	}
	return entries, nil
}
