package lifecycle_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	dockernetwork "github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/lifecycle"
)

// ── Registry tests ───────────────────────────────────────────────────────────

func TestRegistry_RegisterAndList(t *testing.T) {
	reg := lifecycle.NewRegistry()
	spec := lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "registry.example.com/mqtt:v1.0.0", Env: []string{"LOG_LEVEL=info"}}
	reg.Register(spec)

	entries := reg.List()
	require.Len(t, entries, 1)
	assert.Equal(t, "mqtt-01", entries[0].Spec.ID)
	assert.Equal(t, "registry.example.com/mqtt:v1.0.0", entries[0].Spec.Image)
	assert.False(t, entries[0].Running)
}

func TestRegistry_UpdateStatus(t *testing.T) {
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "registry.example.com/mqtt:v1.0.0"})
	reg.SetRunning("mqtt-01", "abc123", true)

	entries := reg.List()
	require.Len(t, entries, 1)
	assert.True(t, entries[0].Running)
	assert.Equal(t, "abc123", entries[0].ContainerID)
}

func TestRegistry_Remove(t *testing.T) {
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "img:v1"})
	reg.Remove("mqtt-01")
	assert.Empty(t, reg.List())
}

// ── Lifecycle Manager tests ──────────────────────────────────────────────────

// TestManager_Start: Start creates a container, starts it, and marks it running in registry.
func TestManager_Start(t *testing.T) {
	ctx := context.Background()
	mock := newMockDocker("container-abc")
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "registry.example.com/mqtt:v1.0.0", Env: []string{"X=1"}})

	mgr := lifecycle.NewManager(mock, reg)
	err := mgr.Start(ctx, "mqtt-01")
	require.NoError(t, err)

	assert.Equal(t, 1, mock.calls("create"))
	assert.Equal(t, 1, mock.calls("start"))

	entries := reg.List()
	require.Len(t, entries, 1)
	assert.True(t, entries[0].Running)
	assert.Equal(t, "container-abc", entries[0].ContainerID)
}

// TestManager_Stop: Stop calls ContainerStop and marks connector not-running.
func TestManager_Stop(t *testing.T) {
	ctx := context.Background()
	mock := newMockDocker("ctr-xyz")
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "img:v1"})

	mgr := lifecycle.NewManager(mock, reg)
	require.NoError(t, mgr.Start(ctx, "mqtt-01"))
	require.NoError(t, mgr.Stop(ctx, "mqtt-01"))

	assert.Equal(t, 1, mock.calls("stop"))
	entries := reg.List()
	assert.False(t, entries[0].Running)
}

// TestManager_Restart: Restart = Stop old container + Start new container.
func TestManager_Restart(t *testing.T) {
	ctx := context.Background()
	mock := newMockDocker("ctr-1")
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "img:v1"})

	mgr := lifecycle.NewManager(mock, reg)
	require.NoError(t, mgr.Start(ctx, "mqtt-01"))

	mock.setNextID("ctr-2")
	require.NoError(t, mgr.Restart(ctx, "mqtt-01"))

	assert.Equal(t, 2, mock.calls("create")) // initial + restart
	assert.Equal(t, 1, mock.calls("stop"))
	assert.Equal(t, 1, mock.calls("remove"))

	entries := reg.List()
	assert.True(t, entries[0].Running)
	assert.Equal(t, "ctr-2", entries[0].ContainerID)
}

// TestManager_Upgrade: Upgrade pulls new image, stops old container, starts new one.
func TestManager_Upgrade(t *testing.T) {
	ctx := context.Background()
	mock := newMockDocker("ctr-old")
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "img:v1"})

	mgr := lifecycle.NewManager(mock, reg)
	require.NoError(t, mgr.Start(ctx, "mqtt-01"))

	mock.setNextID("ctr-new")
	err := mgr.Upgrade(ctx, "mqtt-01", "img:v2")
	require.NoError(t, err)

	assert.Equal(t, 1, mock.calls("pull"))
	assert.Equal(t, 1, mock.calls("stop"))
	assert.Equal(t, 1, mock.calls("remove"))
	assert.Equal(t, 2, mock.calls("create")) // old + new
	assert.Equal(t, 2, mock.calls("start"))  // old + new

	entries := reg.List()
	assert.True(t, entries[0].Running)
	assert.Equal(t, "img:v2", entries[0].Spec.Image)
}

// TestManager_Start_UnknownConnector: Start on unknown ID returns error.
func TestManager_Start_UnknownConnector(t *testing.T) {
	mock := newMockDocker("x")
	reg := lifecycle.NewRegistry()
	mgr := lifecycle.NewManager(mock, reg)
	err := mgr.Start(context.Background(), "does-not-exist")
	require.Error(t, err)
	assert.Equal(t, 0, mock.calls("create"))
}

// TestManager_WatchRestartsStoppedContainer: Watch loop detects dead container and restarts it.
func TestManager_WatchRestartsStoppedContainer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mock := newMockDocker("ctr-init")
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "img:v1"})

	mgr := lifecycle.NewManager(mock, reg)
	require.NoError(t, mgr.Start(ctx, "mqtt-01"))

	// Simulate the container dying
	mock.setInspectRunning(false)
	mock.setNextID("ctr-restarted")

	go mgr.Watch(ctx, 50*time.Millisecond)

	assert.Eventually(t, func() bool {
		entries := reg.List()
		return len(entries) > 0 && entries[0].ContainerID == "ctr-restarted"
	}, 3*time.Second, 50*time.Millisecond, "container should be auto-restarted")
}

// ── Health tests ─────────────────────────────────────────────────────────────

// TestHealth_ContainsGatewayAndConnectors: Snapshot includes system stats and connector liveness.
func TestHealth_ContainsGatewayAndConnectors(t *testing.T) {
	mock := newMockDocker("ctr-abc")
	mock.setInspectRunning(true)
	reg := lifecycle.NewRegistry()
	reg.Register(lifecycle.ConnectorSpec{ID: "mqtt-01", Image: "img:v1"})
	reg.SetRunning("mqtt-01", "ctr-abc", true)

	mon := lifecycle.NewHealthMonitor(mock, reg)
	h := mon.Snapshot()

	assert.Greater(t, h.UptimeSeconds, 0.0)
	require.Len(t, h.Connectors, 1)
	assert.Equal(t, "mqtt-01", h.Connectors[0].ID)
	assert.True(t, h.Connectors[0].Running)
}

// ── mock Docker client ────────────────────────────────────────────────────────

type mockDocker struct {
	mu             sync.Mutex
	nextID         string
	inspectRunning bool
	callCount      map[string]int
	pullErr        error
}

func newMockDocker(initialID string) *mockDocker {
	return &mockDocker{
		nextID:    initialID,
		callCount: make(map[string]int),
	}
}

func (m *mockDocker) setNextID(id string) {
	m.mu.Lock()
	m.nextID = id
	m.mu.Unlock()
}

func (m *mockDocker) setInspectRunning(v bool) {
	m.mu.Lock()
	m.inspectRunning = v
	m.mu.Unlock()
}

func (m *mockDocker) calls(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount[op]
}

func (m *mockDocker) inc(op string) {
	m.callCount[op]++
}

func (m *mockDocker) ContainerCreate(_ context.Context, _ *container.Config, _ *container.HostConfig, _ *dockernetwork.NetworkingConfig, _ *ocispec.Platform, _ string) (container.CreateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inc("create")
	return container.CreateResponse{ID: m.nextID}, nil
}

func (m *mockDocker) ContainerStart(_ context.Context, _ string, _ container.StartOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inc("start")
	m.inspectRunning = true
	return nil
}

func (m *mockDocker) ContainerStop(_ context.Context, _ string, _ container.StopOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inc("stop")
	m.inspectRunning = false
	return nil
}

func (m *mockDocker) ContainerRemove(_ context.Context, _ string, _ container.RemoveOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inc("remove")
	return nil
}

func (m *mockDocker) ContainerInspect(_ context.Context, id string) (container.InspectResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id == "" {
		return container.InspectResponse{}, errors.New("no container")
	}
	state := &container.State{Running: m.inspectRunning}
	return container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{State: state}}, nil
}

func (m *mockDocker) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inc("pull")
	if m.pullErr != nil {
		return nil, m.pullErr
	}
	return io.NopCloser(strings.NewReader("")), nil
}
