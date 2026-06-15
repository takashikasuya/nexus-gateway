package lifecycle

import (
	"context"
	"runtime"
	"runtime/metrics"
	"sync"
	"time"
)

// ConnectorHealth is the liveness status of one connector.
type ConnectorHealth struct {
	ID          string
	Image       string
	PrevImage   string // previous digest-pinned image, set if rollback is available (ADR-0006)
	ContainerID string
	Running     bool
}

// GatewayHealth is a point-in-time health snapshot.
type GatewayHealth struct {
	UptimeSeconds float64
	GoRoutines    int
	MemAllocMB    float64
	DiskUsedMB    float64
	DiskTotalMB   float64
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

	diskUsed, diskTotal := diskStatsMB()

	snap := GatewayHealth{
		UptimeSeconds: time.Since(h.startTime).Seconds(),
		GoRoutines:    runtime.NumGoroutine(),
		MemAllocMB:    allocMB,
		DiskUsedMB:    diskUsed,
		DiskTotalMB:   diskTotal,
	}

	statuses := h.registry.List()
	connectors := make([]ConnectorHealth, len(statuses))
	var wg sync.WaitGroup
	for i, status := range statuses {
		connectors[i] = ConnectorHealth{
			ID:          status.Spec.ID,
			Image:       status.Spec.Image,
			PrevImage:   status.Spec.PrevImage,
			ContainerID: status.ContainerID,
			Running:     status.Running,
		}
		if status.ContainerID != "" && h.docker != nil {
			wg.Add(1)
			go func(idx int, containerID string) {
				defer wg.Done()
				// Verify liveness by inspecting the container in the Docker daemon.
				if resp, err := h.docker.ContainerInspect(ctx, containerID); err == nil {
					connectors[idx].Running = resp.ContainerJSONBase != nil && resp.State != nil && resp.State.Running
				} else {
					connectors[idx].Running = false
				}
			}(i, status.ContainerID)
		}
	}
	wg.Wait()
	snap.Connectors = connectors
	return snap
}
