// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package pointsync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/provisioning"
)

// Result reports the outcome of Service.Start.
type Result struct {
	// Converged is true when the resolver holds at least one entry after the
	// initial load attempt (from a provisioning sync, a persisted snapshot, or
	// the fixture bootstrap). False means the gateway is starting with an
	// empty Point List — every Common Event will resolve as a point-list miss
	// (ADR-0002) until a later sync converges.
	Converged bool
}

// Service owns the Point List convergence lifecycle (ADR-0003, FEAT-031):
// fixture bootstrap → provisioning sync override → blocking first-load →
// forward + reverse resolution, kept live via poll + revalidate hints
// (EgressDown.point_list_update). It is the single owning module for what was
// previously split across pointlist.SyncedResolver, pointsync.Loop, and
// bootstrap sequencing in cmd/gateway/main.go.
//
// A nil client means "fixture only" (dev / no provisioning API); a non-nil
// client always overrides the fixture — the fixture is not consulted at all
// once a client is configured.
type Service struct {
	client      provisioning.Client
	fixturePath string
	cfg         Config
	revalidate  <-chan struct{}
	resolver    *pointlist.SyncedResolver
}

// NewService creates a Service. client may be nil to bootstrap from
// fixturePath only. revalidate may be nil to disable push-triggered re-sync.
func NewService(client provisioning.Client, fixturePath string, cfg Config, revalidate <-chan struct{}) *Service {
	return &Service{
		client:      client,
		fixturePath: fixturePath,
		cfg:         cfg,
		revalidate:  revalidate,
		resolver:    pointlist.NewSynced(nil),
	}
}

// Resolver returns the live resolver, satisfying both pointlist.Resolver
// (forward, consumed by the Normalizer) and pointlist.ReverseResolver
// (reverse, consumed by control Dispatch).
func (s *Service) Resolver() *pointlist.SyncedResolver {
	return s.resolver
}

// Start converges the Point List and returns once the initial load attempt is
// resolved: either the fixture is loaded (no client configured), or the first
// provisioning sync completes, or Config.FirstLoadTimeout elapses — whichever
// comes first. In provisioning-client mode the sync loop keeps running in the
// background (bound to ctx) after Start returns, applying diffs on its poll
// cadence and on revalidate signals.
//
// Start returns an error only for a fatal bootstrap failure (fixture missing
// or unreadable in fixture-only mode, or ctx cancelled while waiting). A
// timed-out or empty initial provisioning sync is reported via
// Result.Converged == false, not an error — the loop keeps retrying on its
// own cadence, matching the previous main.go behavior.
func (s *Service) Start(ctx context.Context) (Result, error) {
	if s.cfg.PersistPath != "" {
		if err := os.MkdirAll(filepath.Dir(s.cfg.PersistPath), 0o755); err != nil {
			return Result{}, fmt.Errorf("pointsync: persist dir create: %w", err)
		}
	}

	if s.client == nil {
		entries, err := loadFixtureEntries(s.fixturePath)
		if err != nil {
			return Result{}, fmt.Errorf("pointsync: load fixture point list: %w", err)
		}
		s.resolver.Update(entries)
		return Result{Converged: len(entries) > 0}, nil
	}

	timeout := s.cfg.FirstLoadTimeout
	if timeout <= 0 {
		timeout = DefaultFirstLoadTimeout
	}

	loop := New(s.client, s.resolver, s.cfg)
	if s.revalidate != nil {
		loop = loop.WithRevalidate(s.revalidate)
	}
	go loop.Run(ctx)

	select {
	case <-loop.Ready():
	case <-time.After(timeout):
		slog.Warn("pointsync: initial sync did not complete within timeout — continuing with whatever the resolver holds; sync keeps retrying in the background", "timeout", timeout)
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}

	snapshot := s.resolver.Snapshot()
	if len(snapshot) == 0 {
		// Proceeding with an empty resolver means every Common Event resolves to a
		// point-list miss and is dropped (ADR-0002). Make that loud rather than silent.
		// This covers both cases: the first sync attempt timed out, or it completed
		// but the provisioning source itself returned an empty Point List.
		slog.Error("point list: resolver is empty after the initial load attempt — starting with an empty Point List; telemetry will be dropped as point-list misses until sync succeeds")
	}

	return Result{Converged: len(snapshot) > 0}, nil
}

// loadFixtureEntries reads a JSON-encoded []pointlist.Entry bootstrap file.
func loadFixtureEntries(path string) ([]pointlist.Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []pointlist.Entry
	return entries, json.Unmarshal(data, &entries)
}
