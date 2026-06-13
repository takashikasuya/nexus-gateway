package pointsync_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/pointsync"
	"nexus-gateway/internal/provisioning"
)

// TestLoop_NoFetchWhenVersionUnchanged verifies that the mock API is called
// for version check but NOT for snapshot when the token hasn't changed.
func TestLoop_NoFetchWhenVersionUnchanged(t *testing.T) {
	mock := provisioning.NewMock([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1"},
	})

	resolver := pointlist.NewSynced(nil)
	cfg := pointsync.Config{Interval: 20 * time.Millisecond, PersistPath: ""}
	loop := pointsync.New(mock, resolver, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	loop.Run(ctx)

	// Version was fetched multiple times but snapshot only once (on first load)
	assert.GreaterOrEqual(t, mock.VersionCalls(), 2, "should poll version multiple times")
	assert.Equal(t, 1, mock.SnapshotCalls(), "snapshot fetched only once (first load)")
}

// TestLoop_FetchOnVersionChange verifies reconvergence when version token changes.
func TestLoop_FetchOnVersionChange(t *testing.T) {
	mock := provisioning.NewMock([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1"},
	})
	resolver := pointlist.NewSynced(nil)
	cfg := pointsync.Config{Interval: 20 * time.Millisecond, PersistPath: ""}
	loop := pointsync.New(mock, resolver, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	// Run loop briefly so first snapshot is loaded
	go loop.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := resolver.Resolve("c1", "l1")
		return ok
	}, 500*time.Millisecond, 10*time.Millisecond, "initial mapping must load")

	// Update snapshot and bump version
	mock.SetSnapshot([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1-updated"},
	})

	// Wait for loop to pick up the change
	require.Eventually(t, func() bool {
		pid, _ := resolver.Resolve("c1", "l1")
		return pid == "p1-updated"
	}, 500*time.Millisecond, 10*time.Millisecond, "resolver must reconverge after version bump")

	assert.Equal(t, 2, mock.SnapshotCalls(), "snapshot fetched twice: initial + after version change")
}

// TestLoop_PersistsAndLoadsOnRestart verifies that synced state survives restart.
func TestLoop_PersistsAndLoadsOnRestart(t *testing.T) {
	path := t.TempDir() + "/pl.json"
	mock := provisioning.NewMock([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1"},
	})

	// First run: load and persist
	r1 := pointlist.NewSynced(nil)
	cfg := pointsync.Config{Interval: 20 * time.Millisecond, PersistPath: path}
	loop1 := pointsync.New(mock, r1, cfg)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()
	loop1.Run(ctx1)

	pid, ok := r1.Resolve("c1", "l1")
	require.True(t, ok)
	assert.Equal(t, "p1", pid)

	// Second run: resolver should load persisted state before first poll
	r2 := pointlist.NewSynced(nil)
	loop2 := pointsync.New(mock, r2, cfg)
	_ = loop2 // loaded on New or on first Run before polling

	// Simulate "gateway restart" — Load persisted file
	require.NoError(t, r2.Load(path))
	pid2, ok2 := r2.Resolve("c1", "l1")
	require.True(t, ok2)
	assert.Equal(t, "p1", pid2)
}
