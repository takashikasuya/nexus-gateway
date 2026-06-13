package lifecycle

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	dockernetwork "github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// ContainerClient is the Docker Engine subset the Manager needs.
type ContainerClient interface {
	ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *dockernetwork.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error
	ContainerInspect(ctx context.Context, containerID string) (container.InspectResponse, error)
	ImagePull(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error)
}

// Manager orchestrates connector container lifecycle via the Docker Engine API.
type Manager struct {
	docker   ContainerClient
	registry *Registry
}

func NewManager(docker ContainerClient, registry *Registry) *Manager {
	return &Manager{docker: docker, registry: registry}
}

// Start creates and starts a container for the given connector ID.
func (m *Manager) Start(ctx context.Context, id string) error {
	status, ok := m.registry.Get(id)
	if !ok {
		return fmt.Errorf("lifecycle: connector %q not in registry", id)
	}
	containerID, err := m.create(ctx, status.Spec)
	if err != nil {
		return fmt.Errorf("lifecycle: create container for %q: %w", id, err)
	}
	if err := m.docker.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("lifecycle: start container for %q: %w", id, err)
	}
	m.registry.SetRunning(id, containerID, true)
	slog.Info("lifecycle: connector started", "id", id, "container", containerID)
	return nil
}

// Stop stops the container for the given connector ID.
func (m *Manager) Stop(ctx context.Context, id string) error {
	status, ok := m.registry.Get(id)
	if !ok {
		return fmt.Errorf("lifecycle: connector %q not in registry", id)
	}
	if status.ContainerID == "" {
		return nil // already stopped
	}
	timeout := 10
	if err := m.docker.ContainerStop(ctx, status.ContainerID, container.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("lifecycle: stop container for %q: %w", id, err)
	}
	m.registry.SetRunning(id, "", false)
	slog.Info("lifecycle: connector stopped", "id", id)
	return nil
}

// Restart stops the existing container and starts a new one.
func (m *Manager) Restart(ctx context.Context, id string) error {
	status, ok := m.registry.Get(id)
	if !ok {
		return fmt.Errorf("lifecycle: connector %q not in registry", id)
	}
	if status.ContainerID != "" {
		if err := m.stopAndRemove(ctx, status.ContainerID); err != nil {
			return fmt.Errorf("lifecycle: restart stop phase for %q: %w", id, err)
		}
		m.registry.SetRunning(id, "", false)
	}
	return m.Start(ctx, id)
}

// Upgrade pulls a new image, stops the current container, and starts a new one with the new image.
func (m *Manager) Upgrade(ctx context.Context, id, newImage string) error {
	_, ok := m.registry.Get(id)
	if !ok {
		return fmt.Errorf("lifecycle: connector %q not in registry", id)
	}

	// Pull new image before stopping the running container.
	rc, err := m.docker.ImagePull(ctx, newImage, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("lifecycle: pull image %q: %w", newImage, err)
	}
	io.Copy(io.Discard, rc) //nolint:errcheck
	rc.Close()

	// Capture container ID before Stop clears it from the registry.
	status, _ := m.registry.Get(id)
	oldContainerID := status.ContainerID

	if err := m.Stop(ctx, id); err != nil {
		return err
	}
	if oldContainerID != "" {
		_ = m.docker.ContainerRemove(ctx, oldContainerID, container.RemoveOptions{})
	}

	// Update spec with new image and start.
	old, _ := m.registry.Get(id)
	m.registry.Register(ConnectorSpec{ID: id, Image: newImage, Env: old.Spec.Env})
	return m.Start(ctx, id)
}

// Watch polls the Docker daemon for connector container liveness at the given interval,
// auto-restarting any that have stopped. Runs until ctx is cancelled.
func (m *Manager) Watch(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, status := range m.registry.List() {
				if !status.Running || status.ContainerID == "" {
					continue
				}
				resp, err := m.docker.ContainerInspect(ctx, status.ContainerID)
				if err != nil || (resp.ContainerJSONBase != nil && resp.State != nil && !resp.State.Running) {
					slog.Warn("lifecycle: connector container stopped — restarting", "id", status.Spec.ID)
					m.registry.SetRunning(status.Spec.ID, "", false)
					if restartErr := m.Start(ctx, status.Spec.ID); restartErr != nil {
						slog.Error("lifecycle: restart failed", "id", status.Spec.ID, "err", restartErr)
					}
				}
			}
		}
	}
}

func (m *Manager) create(ctx context.Context, spec ConnectorSpec) (string, error) {
	resp, err := m.docker.ContainerCreate(ctx,
		&container.Config{
			Image: spec.Image,
			Env:   spec.Env,
		},
		&container.HostConfig{},
		nil, nil,
		"nexus-"+spec.ID,
	)
	if err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (m *Manager) stopAndRemove(ctx context.Context, containerID string) error {
	timeout := 10
	if err := m.docker.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}); err != nil {
		return err
	}
	return m.docker.ContainerRemove(ctx, containerID, container.RemoveOptions{})
}
