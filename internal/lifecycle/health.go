package lifecycle

import (
	"context"
	"runtime"
	"time"
)

var startTime = time.Now()

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
	docker   ContainerClient
	registry *Registry
}

func NewHealthMonitor(docker ContainerClient, registry *Registry) *HealthMonitor {
	return &HealthMonitor{docker: docker, registry: registry}
}

// Snapshot returns a point-in-time health snapshot.
func (h *HealthMonitor) Snapshot() GatewayHealth {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	snap := GatewayHealth{
		UptimeSeconds: time.Since(startTime).Seconds(),
		GoRoutines:    runtime.NumGoroutine(),
		MemAllocMB:    float64(ms.Alloc) / 1024 / 1024,
	}

	for _, status := range h.registry.List() {
		ch := ConnectorHealth{ID: status.Spec.ID, Running: status.Running}
		if status.ContainerID != "" {
			// Verify liveness by inspecting the container in the Docker daemon.
			if resp, err := h.docker.ContainerInspect(context.Background(), status.ContainerID); err == nil {
				ch.Running = resp.ContainerJSONBase != nil && resp.State != nil && resp.State.Running
			} else {
				ch.Running = false
			}
		}
		snap.Connectors = append(snap.Connectors, ch)
	}
	return snap
}
