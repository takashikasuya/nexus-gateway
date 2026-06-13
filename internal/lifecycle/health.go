package lifecycle

import (
	"context"
	"runtime"
	"runtime/metrics"
	"time"
)

// ConnectorHealth is the liveness status of one connector.
type ConnectorHealth struct {
	ID      string
	Running bool
}

// GatewayHealth is a point-in-time health snapshot.
type GatewayHealth struct {
	UptimeSeconds float64
	GoRoutines    int
	MemAllocMB    float64
	Connectors    []ConnectorHealth
}

// HealthMonitor produces health snapshots for the gateway and its connectors.
type HealthMonitor struct {
	docker    ContainerClient
	registry  *Registry
	startTime time.Time // when this monitor was created; per-instance, not package-global
}

func NewHealthMonitor(docker ContainerClient, registry *Registry) *HealthMonitor {
	return &HealthMonitor{docker: docker, registry: registry, startTime: time.Now()}
}

// Snapshot returns a point-in-time health snapshot. The ctx is propagated to
// Docker ContainerInspect calls so callers can enforce deadlines.
func (h *HealthMonitor) Snapshot(ctx context.Context) GatewayHealth {
	// Use runtime/metrics instead of runtime.ReadMemStats to avoid a stop-the-world pause.
	samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples)
	var allocMB float64
	if samples[0].Value.Kind() == metrics.KindUint64 {
		allocMB = float64(samples[0].Value.Uint64()) / 1024 / 1024
	}

	snap := GatewayHealth{
		UptimeSeconds: time.Since(h.startTime).Seconds(),
		GoRoutines:    runtime.NumGoroutine(),
		MemAllocMB:    allocMB,
	}

	for _, status := range h.registry.List() {
		ch := ConnectorHealth{ID: status.Spec.ID, Running: status.Running}
		if status.ContainerID != "" {
			// Verify liveness by inspecting the container in the Docker daemon.
			if resp, err := h.docker.ContainerInspect(ctx, status.ContainerID); err == nil {
				ch.Running = resp.ContainerJSONBase != nil && resp.State != nil && resp.State.Running
			} else {
				ch.Running = false
			}
		}
		snap.Connectors = append(snap.Connectors, ch)
	}
	return snap
}
