// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package dtdpf_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/dtdpf"
	"nexus-gateway/internal/telemetry"
)

func TestLiveEventHubsSend(t *testing.T) {
	if os.Getenv("DTDPF_LIVE_TEST") != "true" {
		t.Skip("set DTDPF_LIVE_TEST=true to send one Event Hubs probe")
	}
	connectionString := os.Getenv("DTDPF_EVENTHUB_CONNECTION_STRING")
	eventHub := os.Getenv("DTDPF_EVENTHUB_NAME")
	require.NotEmpty(t, connectionString)
	require.NotEmpty(t, eventHub)

	transport := dtdpf.EventHubsTransport(os.Getenv("DTDPF_EVENTHUB_TRANSPORT"))
	if transport == "" {
		transport = dtdpf.TransportAMQPTCP
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sink, err := dtdpf.NewEventHubsSinkWithTransport(connectionString, eventHub, transport)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		require.NoError(t, sink.Close(closeCtx))
	})

	record := &telemetry.Record{
		EventID:   uuid.NewString(),
		PointID:   "nexus-gateway-connection-test",
		Value:     float64(time.Now().Unix()),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		DTDPF: &telemetry.DTDPFMetadata{
			RootID: 5,
			DTID:   "nexus-gateway-connection-test",
			Topic:  "nexus-gateway/validation/eventhubs",
		},
	}
	require.NoError(t, sink.Send(ctx, record))
	accepted, err := sink.Checkpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), accepted)
	t.Logf("sent DTDPF Event Hubs probe id=%s hub=%s", record.EventID, eventHub)
}
