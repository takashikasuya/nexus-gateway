package provisioning

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"nexus-gateway/internal/pointlist"
)

// Mock is an in-memory provisioning client for tests and development.
// Call SetSnapshot to update the snapshot and bump the version token atomically.
type Mock struct {
	mu            sync.Mutex
	entries       []pointlist.Entry
	version       string
	versionCalls  atomic.Int64
	snapshotCalls atomic.Int64
}

// NewMock creates a Mock with an initial snapshot.
func NewMock(entries []pointlist.Entry) *Mock {
	m := &Mock{}
	m.setSnapshotLocked(entries)
	return m
}

// SetSnapshot replaces the snapshot and updates the version token.
func (m *Mock) SetSnapshot(entries []pointlist.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setSnapshotLocked(entries)
}

func (m *Mock) setSnapshotLocked(entries []pointlist.Entry) {
	m.entries = make([]pointlist.Entry, len(entries))
	copy(m.entries, entries)
	// Version is a hash of the serialised entries so it changes iff content changes.
	data, _ := json.Marshal(entries)
	m.version = fmt.Sprintf("%x", sha256.Sum256(data))[:16]
}

func (m *Mock) VersionToken(_ context.Context) (string, error) {
	m.versionCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version, nil
}

func (m *Mock) Snapshot(_ context.Context) ([]pointlist.Entry, error) {
	m.snapshotCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]pointlist.Entry, len(m.entries))
	copy(out, m.entries)
	return out, nil
}

// VersionCalls returns the number of VersionToken calls made so far.
func (m *Mock) VersionCalls() int { return int(m.versionCalls.Load()) }

// SnapshotCalls returns the number of Snapshot calls made so far.
func (m *Mock) SnapshotCalls() int { return int(m.snapshotCalls.Load()) }
