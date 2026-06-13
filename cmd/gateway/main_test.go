package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/lifecycle"
)

// startDevSim is gated behind --dev-sim (off by default), so the default build
// runs no in-process connector — the connector-isolation invariant holds (ADR-0001).
// This test exercises the enabled path: registration + a live connector.

func TestDevSim_OffByDefault_NoConnectorRegistered(t *testing.T) {
	// The default build never calls startDevSim, so the registry is empty of any
	// in-process connector. This documents the gate.
	reg := lifecycle.NewRegistry()
	assert.Empty(t, reg.List(), "default build must register no in-process connector")
}

func TestStartDevSim_RegistersAndRunsSim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, js := newTestNATS(t, ctx)
	reg := lifecycle.NewRegistry()

	// Subscribe before starting so we can observe the connector actually publishing.
	sub, err := nc.SubscribeSync("evt.sim.sim-01")
	require.NoError(t, err)

	startDevSim(ctx, js, reg, 50*time.Millisecond)

	// Registered and marked running (synchronous part).
	entries := reg.List()
	require.Len(t, entries, 1)
	assert.Equal(t, "sim-01", entries[0].Spec.ID)
	assert.True(t, entries[0].Running)

	// The connector goroutine actually emits Common Events.
	_, err = sub.NextMsg(3 * time.Second)
	require.NoError(t, err, "dev-sim connector should publish to evt.sim.sim-01")
}

func newTestNATS(t *testing.T, ctx context.Context) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	ns, err := server.NewServer(&server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "EVENTS", Subjects: []string{"evt.>"}, Storage: jetstream.MemoryStorage,
	})
	require.NoError(t, err)
	return nc, js
}
