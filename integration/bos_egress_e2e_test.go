package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/dispatch"
	"nexus-gateway/internal/egress"
	"nexus-gateway/internal/pointlist"
)

// TestE2E_BosControlGate verifies M8: the Building OS HTTP API enforces the
// writable flag — writable points return 202, non-writable return 403.
//
// This does not require GatewayEgressService on the BOS side; it exercises the
// HTTP API's authorization gate independently of the gRPC control path.
//
// Run with:
//
//	E2E_BOS_API_URL=http://localhost:5000 \
//	  go test ./integration/... -run TestE2E_BosControlGate -v -timeout 30s
//
// The test skips automatically when E2E_BOS_API_URL is unset (normal CI, ADR-0004).
func TestE2E_BosControlGate(t *testing.T) {
	apiBase := os.Getenv("E2E_BOS_API_URL")
	if apiBase == "" {
		t.Skip("E2E_BOS_API_URL not set — set E2E_BOS_API_URL to run")
	}

	t.Run("M8: writable point accepted (202)", func(t *testing.T) {
		// SOS-PT-023: VAV-101 temperature setpoint (writable=true)
		statusCode := bosControl(t, apiBase, "SOS-PT-023", 21.5)
		assert.Contains(t, []int{200, 202}, statusCode,
			"POST /points/SOS-PT-023/control must be accepted by BOS (200 or 202)")
	})

	t.Run("M8: non-writable point rejected (403)", func(t *testing.T) {
		// SOS-PT-001: Entrance Temperature (writable=false)
		statusCode := bosControl(t, apiBase, "SOS-PT-001", 99.0)
		assert.Equal(t, http.StatusForbidden, statusCode,
			"POST to non-writable point must return 403")
	})
}

// TestE2E_BosEgressDispatch verifies M7: the full control round-trip via gRPC
// GatewayEgressService.Connect — BOS sends ControlCommand, egress.Agent
// dispatches to NATS, mock connector acknowledges, ControlResult returned.
//
// NOTE (2026-06-15): Building OS connector-worker (port 5051) does not yet
// implement GatewayEgressService. Set E2E_BOS_EGRESS_ADDR to the address of a
// BOS instance that has the egress gRPC service when testing M7.
//
// Run with:
//
//	E2E_BOS_EGRESS_ADDR=localhost:5051 E2E_BOS_API_URL=http://localhost:5000 \
//	  go test ./integration/... -run TestE2E_BosEgressDispatch -v -timeout 60s
//
// The test skips automatically when E2E_BOS_EGRESS_ADDR is unset.
//
// Prerequisites on the Building OS side:
//   - building-os.gateway-bridge running (port 5052, docker-compose.oss.yaml)
//   - building-os.api configured with GatewayConnectionTypes__Map__GW-SOS-001=bacnet-sim
func TestE2E_BosEgressDispatch(t *testing.T) {
	bosAddr := os.Getenv("E2E_BOS_EGRESS_ADDR")
	if bosAddr == "" {
		t.Skip("E2E_BOS_EGRESS_ADDR not set — requires building-os.gateway-bridge:5052 " +
			"and GatewayConnectionTypes__Map__GW-SOS-001=bacnet-sim on the API server")
	}
	apiBase := os.Getenv("E2E_BOS_API_URL")
	if apiBase == "" {
		apiBase = "http://localhost:5000"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "EVENTS",
		Subjects: []string{"evt.>"},
		Storage:  jetstream.MemoryStorage,
	})
	require.NoError(t, err)

	resolver := pointlist.NewSynced(nil)
	resolver.Update([]pointlist.Entry{
		{
			ConnectorID: "bacnet-01",
			Protocol:    "bacnet",
			LocalID:     "L-023",
			PointID:     "SOS-PT-023",
			Writable:    true,
			DeviceRef:   "SOS-DEV-009",
		},
	})

	d := dispatch.New(nc, resolver, 10*time.Second)

	var writeCount atomic.Int64
	sub, err := nc.Subscribe("cmd.bacnet.bacnet-01", func(msg *nats.Msg) {
		writeCount.Add(1)
		reply := dispatch.ConnectorReply{Success: true, Response: "ok"}
		data, _ := json.Marshal(reply)
		_ = msg.Respond(data)
	})
	require.NoError(t, err)
	t.Cleanup(func() { sub.Unsubscribe() })

	agent := egress.New(bosAddr, "GW-SOS-001", d, insecureCreds(), nil)
	go agent.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	t.Run("M7: control command dispatched through gateway to connector", func(t *testing.T) {
		statusCode := bosControl(t, apiBase, "SOS-PT-023", 21.5)
		assert.Contains(t, []int{200, 202}, statusCode)

		require.Eventually(t, func() bool {
			return writeCount.Load() >= 1
		}, 20*time.Second, 200*time.Millisecond,
			"mock connector must receive the write command from NATS within 20s")
	})
}

// bosControl issues a control request via the Building OS HTTP API and returns the status code.
func bosControl(t *testing.T, apiBase, pointID string, value float64) int {
	t.Helper()
	url := fmt.Sprintf("%s/points/%s/control", apiBase, pointID)
	payload, _ := json.Marshal(map[string]any{"ControlType": "Hono", "Value": value})
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload)) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}
