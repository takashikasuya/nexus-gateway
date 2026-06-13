package pointsync

import (
	"context"
	"log/slog"
	"time"

	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/provisioning"
)

// Config holds the sync loop parameters.
type Config struct {
	Interval    time.Duration
	PersistPath string // path to persist the snapshot; empty = no persistence
}

// Loop polls the provisioning API and keeps a SyncedResolver up to date (ADR-0003).
type Loop struct {
	client   provisioning.Client
	resolver *pointlist.SyncedResolver
	cfg      Config
}

// New creates a Loop. Load persisted snapshot (if any) before calling Run.
func New(client provisioning.Client, resolver *pointlist.SyncedResolver, cfg Config) *Loop {
	return &Loop{client: client, resolver: resolver, cfg: cfg}
}

// Run polls the provisioning API until ctx is cancelled.
// It fetches the snapshot once on startup and then only when the version token changes.
func (l *Loop) Run(ctx context.Context) {
	if l.cfg.PersistPath != "" {
		if err := l.resolver.Load(l.cfg.PersistPath); err != nil {
			slog.Warn("pointsync: load persisted snapshot failed", "err", err)
		}
	}

	var lastVersion string

	tick := time.NewTicker(l.cfg.Interval)
	defer tick.Stop()

	// Force immediate first sync
	l.sync(ctx, &lastVersion)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			l.sync(ctx, &lastVersion)
		}
	}
}

func (l *Loop) sync(ctx context.Context, lastVersion *string) {
	ver, err := l.client.VersionToken(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("pointsync: version token error", "err", err)
		}
		return
	}
	if ver == *lastVersion {
		return
	}
	entries, err := l.client.Snapshot(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("pointsync: snapshot fetch error", "err", err)
		}
		return
	}
	l.resolver.Update(entries)
	*lastVersion = ver
	slog.Info("pointsync: point list updated", "version", ver, "count", len(entries))

	if l.cfg.PersistPath != "" {
		if err := l.resolver.Persist(l.cfg.PersistPath); err != nil {
			slog.Warn("pointsync: persist failed", "err", err)
		}
	}
}
