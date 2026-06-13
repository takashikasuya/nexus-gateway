package mqtt_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	pahoClient "github.com/eclipse/paho.golang/paho"
	mochiauth "github.com/mochi-mqtt/server/v2/hooks/auth"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	natssrv "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mqttconn "nexus-gateway/connector/mqtt"
	"nexus-gateway/internal/common"
)

// TestMQTT_NumericPayload: plain float payload → Common Event on NATS.
func TestMQTT_NumericPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	brokerAddr := startBroker(t)
	js := startNATS(t, ctx)

	conn := mqttconn.New(mqttconn.Config{
		ConnectorID:   "mqtt-01",
		BrokerURL:     "mqtt://" + brokerAddr,
		ClientID:      "nexus-gw-num",
		KeepAlive:     30,
		SessionExpiry: 60,
		Points: []mqttconn.PointConfig{
			{Topic: "sensors/temp", DeviceRef: "dev:ahu-01", Unit: "Cel"},
		},
	}, js)
	go conn.Run(ctx)

	// Wait for connector to subscribe
	time.Sleep(300 * time.Millisecond)

	publishMQTT(t, brokerAddr, "sensors/temp", []byte("22.5"))

	evt := consumeOneEvent(t, ctx, js, "evt.mqtt.mqtt-01")
	assert.Equal(t, "mqtt", evt.Protocol)
	assert.Equal(t, "mqtt-01", evt.ConnectorID)
	assert.Equal(t, "sensors/temp", evt.LocalID)
	assert.InDelta(t, 22.5, evt.Value, 0.001)
	assert.Equal(t, "Cel", evt.Unit)
	assert.Equal(t, "Good", evt.Quality)
}

// TestMQTT_JSONPayload: {"value": N} JSON payload → Common Event.
func TestMQTT_JSONPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	brokerAddr := startBroker(t)
	js := startNATS(t, ctx)

	conn := mqttconn.New(mqttconn.Config{
		ConnectorID:   "mqtt-02",
		BrokerURL:     "mqtt://" + brokerAddr,
		ClientID:      "nexus-gw-json",
		KeepAlive:     30,
		SessionExpiry: 60,
		Points: []mqttconn.PointConfig{
			{Topic: "sensors/co2", DeviceRef: "dev:room-01", Unit: "ppm"},
		},
	}, js)
	go conn.Run(ctx)
	time.Sleep(300 * time.Millisecond)

	payload, _ := json.Marshal(map[string]any{"value": 850.0, "unit": "ppm"})
	publishMQTT(t, brokerAddr, "sensors/co2", payload)

	evt := consumeOneEvent(t, ctx, js, "evt.mqtt.mqtt-02")
	assert.InDelta(t, 850.0, evt.Value, 0.001)
	assert.Equal(t, "sensors/co2", evt.LocalID)
}

// TestMQTT_UnknownTopicDropped: message on unconfigured topic → no event emitted.
func TestMQTT_UnknownTopicDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	brokerAddr := startBroker(t)
	js := startNATS(t, ctx)

	// Connector with no configured topics — subscribes to nothing
	conn := mqttconn.New(mqttconn.Config{
		ConnectorID:   "mqtt-03",
		BrokerURL:     "mqtt://" + brokerAddr,
		ClientID:      "nexus-gw-drop",
		KeepAlive:     30,
		SessionExpiry: 60,
		Points:        []mqttconn.PointConfig{},
	}, js)
	go conn.Run(ctx)

	// Publish on a random topic; connector has no subscription
	time.Sleep(200 * time.Millisecond)
	publishMQTT(t, brokerAddr, "unconfigured/topic", []byte("99.0"))

	// Verify no event arrives within 500ms
	cons, err := js.CreateOrUpdateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable:       "drop-check",
		FilterSubject: "evt.mqtt.mqtt-03",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
	require.NoError(t, err)
	count := 0
	for range msgs.Messages() {
		count++
	}
	assert.Equal(t, 0, count, "no events should be emitted for unconfigured topic")
}

// ── helpers ──────────────────────────────────────────────────────────────────

func startBroker(t *testing.T) string {
	t.Helper()
	s := mochi.New(nil)
	require.NoError(t, s.AddHook(new(mochiauth.AllowHook), nil))

	tcp := listeners.NewTCP(listeners.Config{ID: "t1", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))

	go func() { _ = s.Serve() }()
	t.Cleanup(func() { _ = s.Close() })

	require.Eventually(t, func() bool {
		addr := tcp.Address()
		return addr != "" && addr != "127.0.0.1:0"
	}, 3*time.Second, 10*time.Millisecond)

	return tcp.Address()
}

func startNATS(t *testing.T, ctx context.Context) jetstream.JetStream {
	t.Helper()
	opts := &natssrv.Options{Port: -1, JetStream: true}
	ns, err := natssrv.NewServer(opts)
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
		Name:     "EVENTS",
		Subjects: []string{"evt.>"},
		Storage:  jetstream.MemoryStorage,
	})
	require.NoError(t, err)

	return js
}

func publishMQTT(t *testing.T, brokerAddr, topic string, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := dialPaho(ctx, brokerAddr, fmt.Sprintf("test-pub-%d", time.Now().UnixNano()))
	require.NoError(t, err)
	defer c.Disconnect(&pahoClient.Disconnect{})

	_, err = c.Publish(ctx, &pahoClient.Publish{
		Topic:   topic,
		QoS:     0,
		Payload: payload,
	})
	require.NoError(t, err)
}

func dialPaho(ctx context.Context, addr, clientID string) (*pahoClient.Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := pahoClient.NewClient(pahoClient.ClientConfig{
		Conn:     conn,
		ClientID: clientID,
	})
	_, err = c.Connect(ctx, &pahoClient.Connect{
		KeepAlive:  30,
		ClientID:   clientID,
		CleanStart: true,
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

func consumeOneEvent(t *testing.T, ctx context.Context, js jetstream.JetStream, subject string) common.Event {
	t.Helper()
	cons, err := js.CreateOrUpdateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable:       "test-" + subject[len(subject)-6:],
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(10*time.Second))
	require.NoError(t, err)

	var evt common.Event
	for msg := range msgs.Messages() {
		require.NoError(t, json.Unmarshal(msg.Data(), &evt))
		_ = msg.Ack()
		return evt
	}
	t.Fatal("no event received within timeout")
	return evt
}
