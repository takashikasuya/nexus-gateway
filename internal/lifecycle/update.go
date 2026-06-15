package lifecycle

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"

	"nexus-gateway/internal/catalog"
)

func containerRemoveOpts() container.RemoveOptions { return container.RemoveOptions{} }

// Update atomically replaces a running connector with a new version from a catalog Manifest.
//
// ADR-0006 sequence:
//  1. Validate manifest; compare digest — return nil immediately if unchanged (no pull).
//  2. Pull new digest-pinned image.
//  3. Cosign verify (if SignatureRequired).
//  4. Stop old container.
//  5. Start new container with manifest permissions.
//  6. Health soak: poll ContainerInspect for soakWindow; if container exits → rollback.
//  7. Rollback: start old image without a pull (already local); return wrapped error.
func (m *Manager) Update(
	ctx context.Context,
	id string,
	manifest catalog.Manifest,
	verifier catalog.Verifier,
	allowedRegistries []string,
	gatewayVersion string,
	soakWindow time.Duration,
) error {
	if err := manifest.Validate(allowedRegistries, gatewayVersion); err != nil {
		return fmt.Errorf("lifecycle: update %q: invalid manifest: %w", id, err)
	}

	unlock := m.lockConn(id)
	defer unlock()

	current, ok := m.registry.Get(id)
	if !ok {
		return fmt.Errorf("lifecycle: update %q: %w", id, ErrConnectorNotFound)
	}

	newImageRef := manifest.ImageRef()

	// Skip if the installed digest already matches the catalog manifest.
	if digestFromRef(current.Spec.Image) == manifest.Digest {
		return nil
	}

	// Pull the new digest-pinned image.
	rc, err := m.docker.ImagePull(ctx, newImageRef, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("lifecycle: update %q: pull %q: %w", id, newImageRef, err)
	}
	io.Copy(io.Discard, rc) //nolint:errcheck
	rc.Close()

	// Cosign verify before touching the running container.
	if manifest.SignatureRequired {
		if err := verifier.Verify(ctx, newImageRef); err != nil {
			return fmt.Errorf("lifecycle: update %q: signature verification failed: %w", id, err)
		}
	}

	oldContainerID := current.ContainerID
	oldImage := current.Spec.Image

	// Stop and remove the old container.
	if oldContainerID != "" {
		if err := m.doStop(ctx, id); err != nil {
			return fmt.Errorf("lifecycle: update %q: stop old container: %w", id, err)
		}
		if err := m.docker.ContainerRemove(ctx, oldContainerID, containerRemoveOpts()); err != nil {
			slog.Warn("lifecycle: update: remove old container failed (continuing)", "id", id, "err", err)
		}
	}

	// Register the new spec (preserving old image as PrevImage for rollback).
	newSpec := ConnectorSpec{
		ID:        id,
		Image:     newImageRef,
		PrevImage: oldImage,
		Env:       current.Spec.Env,
		Permissions: ConnectorPermissions{
			Network: manifest.Permissions.Network,
			Mounts:  manifest.Permissions.Mounts,
		},
	}
	m.registry.Register(newSpec)

	// Start the new container.
	if err := m.doStart(ctx, id); err != nil {
		return m.rollback(ctx, id, oldImage, fmt.Errorf("start new container: %w", err))
	}

	// Health soak: verify the new container stays running for soakWindow.
	newStatus, _ := m.registry.Get(id)
	if err := m.soakCheck(ctx, newStatus.ContainerID, soakWindow); err != nil {
		return m.rollback(ctx, id, oldImage, err)
	}

	slog.Info("lifecycle: connector updated", "id", id, "image", newImageRef)
	return nil
}

// rollback restores the previous image without a pull (it is already local).
// It always returns a non-nil error wrapping both the original failure and any rollback error.
func (m *Manager) rollback(ctx context.Context, id, prevImage string, cause error) error {
	slog.Warn("lifecycle: update failed — rolling back", "id", id, "prev_image", prevImage, "cause", cause)

	// Stop the new (failed) container if it is running.
	_ = m.doStop(ctx, id)

	// Restore the previous spec and start without a pull.
	prev, ok := m.registry.Get(id)
	if !ok || prevImage == "" {
		return fmt.Errorf("lifecycle: update %q: rollback: no previous image available: %w", id, cause)
	}
	rollbackSpec := ConnectorSpec{
		ID:          id,
		Image:       prevImage,
		PrevImage:   prev.Spec.PrevImage, // preserve any earlier rollback chain
		Env:         prev.Spec.Env,
		Permissions: prev.Spec.Permissions,
	}
	m.registry.Register(rollbackSpec)

	if startErr := m.doStart(ctx, id); startErr != nil {
		return fmt.Errorf("lifecycle: update %q: rollback failed (connector is stopped): %w; original: %v", id, startErr, cause)
	}

	slog.Info("lifecycle: rollback complete", "id", id, "image", prevImage)
	return fmt.Errorf("lifecycle: update %q: rollback triggered: %w", id, cause)
}

// soakCheck polls ContainerInspect until soakWindow elapses or the container exits.
func (m *Manager) soakCheck(ctx context.Context, containerID string, soakWindow time.Duration) error {
	if containerID == "" || soakWindow <= 0 {
		return nil
	}
	deadline := time.Now().Add(soakWindow)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		resp, err := m.docker.ContainerInspect(ctx, containerID)
		if err != nil {
			return fmt.Errorf("health soak: inspect failed: %w", err)
		}
		if resp.ContainerJSONBase == nil || resp.State == nil || !resp.State.Running {
			return fmt.Errorf("health soak: container exited within soak window")
		}
	}
	return nil
}

// digestFromRef extracts "sha256:…" from "registry/img@sha256:…".
func digestFromRef(ref string) string {
	if idx := strings.Index(ref, "@"); idx >= 0 {
		return ref[idx+1:]
	}
	return ""
}

// ── Updater ───────────────────────────────────────────────────────────────────

// UpdaterConfig holds tuning parameters for the Updater.
type UpdaterConfig struct {
	SoakWindow time.Duration // soak window for new container health check (default: 30s)
}

// Updater polls the Connector Catalog at a configurable interval and applies
// available updates to installed, running connectors (ADR-0006 poll-only model).
type Updater struct {
	mgr       *Manager
	registry  *Registry
	catalog   catalog.Client
	verifier  catalog.Verifier
	allowlist []string
	gwVersion string
	cfg       UpdaterConfig
}

func NewUpdater(mgr *Manager, reg *Registry, cat catalog.Client, verifier catalog.Verifier, allowlist []string, gwVersion string, cfg UpdaterConfig) *Updater {
	if cfg.SoakWindow <= 0 {
		cfg.SoakWindow = 30 * time.Second
	}
	return &Updater{
		mgr:       mgr,
		registry:  reg,
		catalog:   cat,
		verifier:  verifier,
		allowlist: allowlist,
		gwVersion: gwVersion,
		cfg:       cfg,
	}
}

// Run polls the catalog at the given interval and applies updates until ctx is cancelled.
func (u *Updater) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.poll(ctx)
		}
	}
}

func (u *Updater) poll(ctx context.Context) {
	for _, status := range u.registry.List() {
		if !status.Running {
			continue // do not update stopped connectors
		}
		manifest, err := u.catalog.Fetch(ctx, status.Spec.ID)
		if err != nil {
			slog.Warn("updater: catalog fetch failed", "id", status.Spec.ID, "err", err)
			continue
		}
		if err := u.mgr.Update(ctx, status.Spec.ID, manifest, u.verifier, u.allowlist, u.gwVersion, u.cfg.SoakWindow); err != nil {
			slog.Error("updater: update failed", "id", status.Spec.ID, "err", err)
		}
	}
}
