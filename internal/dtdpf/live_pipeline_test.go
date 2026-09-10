// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package dtdpf

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/common"
	"nexus-gateway/internal/normalizer"
	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/storeforward"
	"nexus-gateway/internal/uplink"
)

func TestLiveDTDPFTelemetryPipeline(t *testing.T) {
	if os.Getenv("DTDPF_LIVE_TEST") != "true" {
		t.Skip("set DTDPF_LIVE_TEST=true to run the live DTDPF pipeline")
	}
	connectionString := os.Getenv("DTDPF_EVENTHUB_CONNECTION_STRING")
	eventHub := os.Getenv("DTDPF_EVENTHUB_NAME")
	transport := EventHubsTransport(os.Getenv("DTDPF_EVENTHUB_TRANSPORT"))
	if transport == "" {
		transport = TransportAMQPTCP
	}
	require.NotEmpty(t, connectionString)
	require.NotEmpty(t, eventHub)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	config, err := LoadPointConfig(strings.NewReader(`{
		"pointData":{"5":[{
			"dtId":"nexus-gateway-pipeline-test",
			"topic":"nexus-gateway/validation/pipeline",
			"type":2,
			"protocol":"sim"
		}]}
	}`))
	require.NoError(t, err)
	eventData, err := json.Marshal(common.Event{
		Protocol:    "sim",
		ConnectorID: "live-test-connector",
		LocalID:     "sim://validation/pipeline",
		Value:       float64(time.Now().Unix()),
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
	})
	require.NoError(t, err)
	points := pointlist.NewFixture([]pointlist.Entry{
		{ConnectorID: "live-test-connector", LocalID: "sim://validation/pipeline", PointID: "nexus-gateway-pipeline-test"},
	})
	natsServer, err := server.NewServer(&server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1})
	require.NoError(t, err)
	go natsServer.Start()
	require.True(t, natsServer.ReadyForConnections(5*time.Second))
	defer natsServer.Shutdown()
	natsConnection, err := nats.Connect(natsServer.ClientURL())
	require.NoError(t, err)
	defer natsConnection.Close()
	jetStream, err := jetstream.New(natsConnection)
	require.NoError(t, err)
	_, err = jetStream.CreateStream(ctx, jetstream.StreamConfig{
		Name: "EVENTS", Subjects: []string{"evt.>"}, Storage: jetstream.MemoryStorage,
	})
	require.NoError(t, err)
	normalized, err := normalizer.NewWithMetadata(ctx, jetStream, points, config, "live-test-gateway")
	require.NoError(t, err)

	buffer, err := storeforward.OpenWithPolicy(t.TempDir()+"/sf.db", 100, storeforward.BlockWhenFull)
	require.NoError(t, err)
	defer buffer.Close()
	go storeforward.Pump(ctx, normalized.Records(), buffer)

	eventHubsUplink, err := NewUplink(connectionString, eventHub, transport, buffer, uplink.Config{
		CheckpointSize: 1,
		CheckpointAge:  time.Second,
	})
	require.NoError(t, err)
	go eventHubsUplink.Run(ctx)
	_, err = jetStream.Publish(ctx, "evt.sim.live-test-connector", eventData)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return buffer.Written() == 1 && buffer.Cursor() > 0 && buffer.Sent() == 1 && buffer.Depth() == 0
	}, 45*time.Second, 100*time.Millisecond)
	t.Logf("JetStream-to-DTDPF pipeline delivered one event to hub=%s via=%s cursor=%d", eventHub, transport, buffer.Cursor())
}
