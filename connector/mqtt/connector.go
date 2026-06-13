package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"nexus-gateway/internal/common"
)

// PointConfig describes a single MQTT point: the topic is the native local_id (ADR-0001).
type PointConfig struct {
	Topic           string
	DeviceRef       string
	Unit            string
	Writable        bool   // point accepts write commands
	CommandTopic    string // MQTT topic to publish writes to; required when Writable is true
	PayloadTemplate string // fmt.Sprintf template for the write payload, e.g. `{"present_value": %g}`; defaults to plain float string
}

// Config holds all settings for one MQTT connector instance.
type Config struct {
	ConnectorID   string
	BrokerURL     string // e.g. "mqtt://localhost:1883"
	ClientID      string
	Username      string
	Password      []byte
	KeepAlive     uint16
	SessionExpiry uint32 // seconds; 0 = session ends on disconnect
	Points        []PointConfig
}

// WriteReply is the JSON payload returned by the connector write handler over NATS request-reply.
// It mirrors dispatch.ConnectorReply and is exported so tests and callers can decode it.
type WriteReply struct {
	Success  bool   `json:"success"`
	Response string `json:"response"`
}

// writeRequest mirrors dispatch.WriteRequest — kept local to avoid import cycle.
type writeRequest struct {
	ControlID string  `json:"control_id"`
	LocalID   string  `json:"local_id"`
	DeviceRef string  `json:"device_ref"`
	Value     float64 `json:"value"`
	Priority  int32   `json:"priority"`
}

// Connector subscribes to an MQTT broker and publishes Common Events to NATS JetStream
// on subject evt.mqtt.<connector_id> (ADR-0001, ADR-0005).
// It also handles write commands arriving on cmd.mqtt.<connector_id> via NATS request-reply
// and publishes them to the broker (ADR-0004).
type Connector struct {
	cfg       Config
	nc        *nats.Conn
	js        jetstream.JetStream
	readyOnce sync.Once
	ready     chan struct{}
	dedupMu   sync.Mutex
	dedup     map[string]*WriteReply // nil value = in-flight (reserved); non-nil = completed
}

func New(cfg Config, nc *nats.Conn, js jetstream.JetStream) *Connector {
	return &Connector{
		cfg:   cfg,
		nc:    nc,
		js:    js,
		ready: make(chan struct{}),
		dedup: make(map[string]*WriteReply),
	}
}

// AwaitReady blocks until the first MQTT subscription is active or ctx is cancelled.
// Use this in tests and startup sequences instead of time.Sleep.
func (c *Connector) AwaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run connects to the MQTT broker and processes messages until ctx is cancelled.
// autopaho handles reconnection automatically.
func (c *Connector) Run(ctx context.Context) {
	topicMap := make(map[string]PointConfig, len(c.cfg.Points))
	subs := make([]paho.SubscribeOptions, 0, len(c.cfg.Points))
	for _, p := range c.cfg.Points {
		topicMap[p.Topic] = p
		subs = append(subs, paho.SubscribeOptions{Topic: p.Topic, QoS: 1})
	}

	brokerURL, err := url.Parse(c.cfg.BrokerURL)
	if err != nil {
		slog.Error("mqtt: invalid broker URL", "url", c.cfg.BrokerURL, "err", err)
		return
	}

	subject := "evt.mqtt." + c.cfg.ConnectorID

	// Register write handler before starting the connection so it is live before
	// readyOnce fires (OnConnectionUp runs on autopaho's internal goroutine).
	var cm *autopaho.ConnectionManager
	sub, err := c.nc.Subscribe("cmd.mqtt."+c.cfg.ConnectorID, func(msg *nats.Msg) {
		// Each write runs in its own goroutine so the NATS dispatch goroutine is
		// never blocked by the up-to-8 s cm.Publish call.
		go c.handleWrite(ctx, cm, topicMap, msg)
	})
	if err != nil {
		slog.Error("mqtt: write handler subscribe failed", "err", err)
		return
	}
	defer sub.Unsubscribe()

	cm, err = autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     c.cfg.KeepAlive,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         c.cfg.SessionExpiry,
		ConnectRetryDelay:             5 * time.Second,
		ConnectUsername:               c.cfg.Username,
		ConnectPassword:               c.cfg.Password,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			if len(subs) > 0 {
				if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs}); err != nil {
					slog.Error("mqtt: subscribe failed", "err", err)
					return
				}
			}
			// Signal that the first subscription is ready (subsequent reconnects are silently ignored).
			c.readyOnce.Do(func() { close(c.ready) })
		},
		ClientConfig: paho.ClientConfig{
			ClientID: c.cfg.ClientID,
			// Manual acknowledgment: PUBACK is sent only after the event lands in JetStream,
			// preventing data loss when NATS is temporarily unavailable (QoS 1 guarantee).
			EnableManualAcknowledgment: true,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					p, ok := topicMap[pr.Packet.Topic]
					if !ok {
						// Unknown topic: ack immediately to avoid infinite broker retry.
						_ = pr.Client.Ack(pr.Packet)
						return true, nil
					}
					value, ok := extractValue(pr.Packet.Payload)
					if !ok {
						slog.Warn("mqtt: unparseable payload", "topic", pr.Packet.Topic)
						// Unparseable: ack to avoid infinite retry; event cannot be used.
						_ = pr.Client.Ack(pr.Packet)
						return true, nil
					}
					evt := common.Event{
						Protocol:    "mqtt",
						ConnectorID: c.cfg.ConnectorID,
						LocalID:     p.Topic,
						DeviceRef:   p.DeviceRef,
						Value:       value,
						Unit:        p.Unit,
						Quality:     "Good",
						Timestamp:   time.Now().UTC().Format(time.RFC3339),
					}
					data, err := json.Marshal(evt)
					if err != nil {
						_ = pr.Client.Ack(pr.Packet)
						return true, nil
					}
					if _, err := c.js.Publish(ctx, subject, data); err != nil {
						slog.Warn("mqtt: nats publish failed — withholding PUBACK for QoS 1 retry", "err", err)
						// Do not ack: broker will redeliver when NATS is available again.
						return true, nil
					}
					_ = pr.Client.Ack(pr.Packet)
					return true, nil
				},
			},
		},
	})
	if err != nil {
		slog.Error("mqtt: connection manager init failed", "err", err)
		return
	}

	<-cm.Done()
}

func (c *Connector) handleWrite(ctx context.Context, cm *autopaho.ConnectionManager, topicMap map[string]PointConfig, msg *nats.Msg) {
	var req writeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		respond(msg, WriteReply{false, "bad_request"})
		return
	}

	// Reserve the slot atomically. nil = in-flight (first goroutine for this control_id),
	// non-nil = completed (return cached result). This prevents a concurrent duplicate
	// goroutine (same control_id) from issuing a second write to the device.
	c.dedupMu.Lock()
	if entry, ok := c.dedup[req.ControlID]; ok {
		c.dedupMu.Unlock()
		if entry == nil {
			// Another goroutine is in-flight for this control_id. Return early; the
			// dispatcher will retry and eventually hit the cached result.
			respond(msg, WriteReply{false, "in_flight"})
		} else {
			respond(msg, *entry)
		}
		return
	}
	c.dedup[req.ControlID] = nil // reserve slot
	c.dedupMu.Unlock()

	p, ok := topicMap[req.LocalID]
	if !ok || !p.Writable || p.CommandTopic == "" {
		reply := WriteReply{false, "not_writable"}
		c.cacheDedup(req.ControlID, reply)
		respond(msg, reply)
		return
	}

	payload := formatPayload(p.PayloadTemplate, req.Value)

	// Use a bounded timeout so we never block past the dispatcher's deadline.
	wCtx, wCancel := context.WithTimeout(ctx, 8*time.Second)
	defer wCancel()

	_, err := cm.Publish(wCtx, &paho.Publish{
		Topic:   p.CommandTopic,
		QoS:     1,
		Payload: payload,
	})

	var reply WriteReply
	if err != nil {
		reply = WriteReply{false, "device_error: " + err.Error()}
	} else {
		reply = WriteReply{true, "ok"}
	}
	c.cacheDedup(req.ControlID, reply)
	respond(msg, reply)
}

func (c *Connector) cacheDedup(controlID string, reply WriteReply) {
	c.dedupMu.Lock()
	c.dedup[controlID] = &reply
	c.dedupMu.Unlock()
}

func respond(msg *nats.Msg, reply WriteReply) {
	data, _ := json.Marshal(reply)
	_ = msg.Respond(data)
}

func formatPayload(tmpl string, value float64) []byte {
	plain := []byte(strconv.FormatFloat(value, 'g', -1, 64))
	if tmpl == "" {
		return plain
	}
	result := fmt.Sprintf(tmpl, value)
	// fmt.Sprintf embeds "%!verb(type=value)" when the verb is wrong for the arg type.
	// Fall back to plain float rather than sending malformed bytes to the device.
	if strings.Contains(result, "%!") {
		slog.Warn("mqtt: bad PayloadTemplate verb — falling back to plain float", "template", tmpl)
		return plain
	}
	return []byte(result)
}

// extractValue extracts a float64 from a raw MQTT payload.
// Supports: plain number ("22.5", "42"), JSON object with a "value" key ({"value": 22.5}).
func extractValue(payload []byte) (float64, bool) {
	s := strings.TrimSpace(string(payload))
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v, true
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return 0, false
	}
	for _, key := range []string{"value", "Value", "v"} {
		if v, ok := obj[key]; ok {
			switch n := v.(type) {
			case float64:
				return n, true
			case string:
				if f, err := strconv.ParseFloat(n, 64); err == nil {
					return f, true
				}
			}
		}
	}
	return 0, false
}
