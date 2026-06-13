package mqtt

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go/jetstream"

	"nexus-gateway/internal/common"
)

// PointConfig describes a single MQTT point: the topic is the native local_id (ADR-0001).
type PointConfig struct {
	Topic     string
	DeviceRef string
	Unit      string
}

// Config holds all settings for one MQTT connector instance.
type Config struct {
	ConnectorID        string
	BrokerURL          string // e.g. "mqtt://localhost:1883"
	ClientID           string
	Username           string
	Password           []byte
	KeepAlive          uint16
	SessionExpiry      uint32 // seconds; 0 = session ends on disconnect
	Points             []PointConfig
}

// Connector subscribes to an MQTT broker and publishes Common Events to NATS JetStream
// on subject evt.mqtt.<connector_id> (ADR-0001, ADR-0005).
type Connector struct {
	cfg Config
	js  jetstream.JetStream
}

func New(cfg Config, js jetstream.JetStream) *Connector {
	return &Connector{cfg: cfg, js: js}
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

	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     c.cfg.KeepAlive,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         c.cfg.SessionExpiry,
		ConnectRetryDelay:             5 * time.Second,
		ConnectUsername:               c.cfg.Username,
		ConnectPassword:               c.cfg.Password,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			if len(subs) == 0 {
				return
			}
			if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs}); err != nil {
				slog.Error("mqtt: subscribe failed", "err", err)
			}
		},
		ClientConfig: paho.ClientConfig{
			ClientID: c.cfg.ClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					p, ok := topicMap[pr.Packet.Topic]
					if !ok {
						return true, nil
					}
					value, ok := extractValue(pr.Packet.Payload)
					if !ok {
						slog.Warn("mqtt: unparseable payload", "topic", pr.Packet.Topic)
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
						return true, nil
					}
					if _, err := c.js.Publish(ctx, subject, data); err != nil {
						slog.Warn("mqtt: nats publish failed", "err", err)
					}
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
