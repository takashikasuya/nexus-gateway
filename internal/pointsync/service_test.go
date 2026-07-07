// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package pointsync_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/pointsync"
	"nexus-gateway/internal/provisioning"
)

// blockingClient is a provisioning.Client whose Fetch never returns on its
// own — it only unblocks when ctx is cancelled. Used to simulate an
// unresponsive/hung provisioning source.
type blockingClient struct{}

func (blockingClient) Fetch(ctx context.Context, _ string) (*provisioning.FetchResult, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func writeFixture(t *testing.T, entries []pointlist.Entry) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "point_list.json")
	data, err := json.Marshal(entries)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

// TestService_FixtureBootstrap verifies the no-client mode loads the fixture
// file and exposes both forward and reverse resolution.
func TestService_FixtureBootstrap(t *testing.T) {
	fixturePath := writeFixture(t, []pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1"},
	})
	svc := pointsync.NewService(nil, fixturePath, pointsync.Config{}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := svc.Start(ctx)
	require.NoError(t, err)
	assert.True(t, result.Converged)

	pid, ok := svc.Resolver().Resolve("c1", "l1")
	require.True(t, ok)
	assert.Equal(t, "p1", pid)

	entry, ok := svc.Resolver().ResolveReverse("p1")
	require.True(t, ok)
	assert.Equal(t, "l1", entry.LocalID)
}

// TestService_ProvisioningClient_OverridesFixture verifies that a configured
// provisioning client always wins over the fixture bootstrap (ADR-0003) —
// the fixture is not even consulted once a client is present.
func TestService_ProvisioningClient_OverridesFixture(t *testing.T) {
	fixturePath := writeFixture(t, []pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "fixture-p1"},
	})
	mock := provisioning.NewMock([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "prov-p1"},
	})
	svc := pointsync.NewService(mock, fixturePath, pointsync.Config{
		Interval:         50 * time.Millisecond,
		FirstLoadTimeout: time.Second,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := svc.Start(ctx)
	require.NoError(t, err)
	assert.True(t, result.Converged)

	pid, ok := svc.Resolver().Resolve("c1", "l1")
	require.True(t, ok)
	assert.Equal(t, "prov-p1", pid, "provisioning must override the fixture bootstrap")
}

// TestService_FirstLoadTimeout_DoesNotBlockStart verifies Start returns
// promptly at FirstLoadTimeout when the provisioning source is unresponsive,
// instead of hanging gateway startup indefinitely.
func TestService_FirstLoadTimeout_DoesNotBlockStart(t *testing.T) {
	svc := pointsync.NewService(blockingClient{}, "", pointsync.Config{
		Interval:         time.Hour,
		FirstLoadTimeout: 50 * time.Millisecond,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	result, err := svc.Start(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.False(t, result.Converged)
	assert.Less(t, elapsed, time.Second, "Start must return promptly at FirstLoadTimeout, not block on a hung provisioning source")
}

// TestService_RevalidateSignal_TriggersResync verifies a send on the
// revalidate channel (EgressDown.point_list_update) forces an immediate
// re-sync rather than waiting for the next poll tick.
func TestService_RevalidateSignal_TriggersResync(t *testing.T) {
	mock := provisioning.NewMock([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1"},
	})
	revalidate := make(chan struct{}, 1)
	svc := pointsync.NewService(mock, "", pointsync.Config{
		Interval:         time.Hour, // long enough that only the revalidate signal drives the 2nd sync
		FirstLoadTimeout: time.Second,
	}, revalidate)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := svc.Start(ctx)
	require.NoError(t, err)
	require.True(t, result.Converged)

	mock.SetEntries([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1-updated"},
	})
	revalidate <- struct{}{}

	require.Eventually(t, func() bool {
		pid, _ := svc.Resolver().Resolve("c1", "l1")
		return pid == "p1-updated"
	}, time.Second, 5*time.Millisecond, "revalidate signal must trigger an immediate re-sync (EgressDown.point_list_update)")
}

// TestService_PersistedSnapshot_LoadedOnNextStart verifies a persisted
// snapshot converges the resolver even when the live provisioning source is
// unresponsive on the next process start.
func TestService_PersistedSnapshot_LoadedOnNextStart(t *testing.T) {
	persistPath := filepath.Join(t.TempDir(), "point_list.json")
	mock := provisioning.NewMock([]pointlist.Entry{
		{ConnectorID: "c1", Protocol: "sim", LocalID: "l1", PointID: "p1"},
	})
	svc1 := pointsync.NewService(mock, "", pointsync.Config{
		Interval:         time.Hour,
		PersistPath:      persistPath,
		FirstLoadTimeout: time.Second,
	}, nil)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	result, err := svc1.Start(ctx1)
	require.NoError(t, err)
	require.True(t, result.Converged)
	require.FileExists(t, persistPath)

	// Second Service, backed by a client that never responds: the persisted
	// snapshot must still populate the resolver without a live sync.
	svc2 := pointsync.NewService(blockingClient{}, "", pointsync.Config{
		Interval:         time.Hour,
		PersistPath:      persistPath,
		FirstLoadTimeout: 100 * time.Millisecond,
	}, nil)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	result2, err := svc2.Start(ctx2)
	require.NoError(t, err)
	assert.True(t, result2.Converged, "persisted snapshot must converge the resolver even when the live provisioning source is unresponsive")

	pid, ok := svc2.Resolver().Resolve("c1", "l1")
	require.True(t, ok)
	assert.Equal(t, "p1", pid)
}

// TestService_FixtureMode_MissingFile_ReturnsError verifies a missing/unreadable
// fixture file is a fatal bootstrap error in fixture-only mode (no provisioning
// client configured to fall back on).
func TestService_FixtureMode_MissingFile_ReturnsError(t *testing.T) {
	svc := pointsync.NewService(nil, filepath.Join(t.TempDir(), "does-not-exist.json"), pointsync.Config{}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := svc.Start(ctx)
	assert.Error(t, err)
}
