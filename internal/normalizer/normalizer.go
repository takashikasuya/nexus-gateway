package normalizer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	pb "nexus-gateway/gen"
	"nexus-gateway/internal/common"
	"nexus-gateway/internal/metrics"
	"nexus-gateway/internal/pointlist"
)

// Outcome classifies a Common Event so the consume loop can drop-and-meter
// poison and point-list-miss events distinctly (ADR-0002 best-effort).
type Outcome int

const (
	OutcomeOK     Outcome = iota // resolved → emit a TelemetryFrame
	OutcomePoison                // unparseable/permanently invalid → Term + meter
	OutcomeMiss                  // unknown local_id → Term + meter
)

// Normalizer is the single durable pull consumer on evt.> (ADR-0001, ADR-0005).
// It resolves native LocalID → canonical PointID via the resolver, then emits
// TelemetryFrames downstream. Unknown local_ids are skipped and metered.
type Normalizer struct {
	frames chan *pb.TelemetryFrame
}

func New(ctx context.Context, js jetstream.JetStream, resolver pointlist.Resolver, gatewayID string) (*Normalizer, error) {
	cons, err := js.CreateOrUpdateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable:       "normalizer",
		FilterSubject: "evt.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,
	})
	if err != nil {
		return nil, fmt.Errorf("create normalizer consumer: %w", err)
	}

	n := &Normalizer{frames: make(chan *pb.TelemetryFrame, 256)}

	go n.consume(ctx, cons, resolver, gatewayID)

	return n, nil
}

// Frames returns the channel of normalized TelemetryFrames.
func (n *Normalizer) Frames() <-chan *pb.TelemetryFrame {
	return n.frames
}

func (n *Normalizer) consume(ctx context.Context, cons jetstream.Consumer, resolver pointlist.Resolver, gatewayID string) {
	defer close(n.frames)
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, err := cons.Fetch(32, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for msg := range msgs.Messages() {
			frame, out := Normalize(msg.Data(), resolver, gatewayID)
			switch out {
			case OutcomePoison:
				// Retrying an unparseable event is pointless; terminate, don't redeliver.
				metrics.IncNormalizerInvalid()
				_ = msg.Term()
				continue
			case OutcomeMiss:
				// The Point List is synced before telemetry flows (ADR-0003), so an
				// unknown local_id is misconfiguration, not a sync race: drop and meter.
				metrics.IncNormalizerUnresolved()
				_ = msg.Term()
				continue
			}
			select {
			case n.frames <- frame:
				_ = msg.Ack()
			case <-ctx.Done():
				_ = msg.Nak()
				return
			}
		}
		if err := msgs.Error(); err != nil {
			slog.Warn("normalizer: fetch error", "err", err)
		}
	}
}

// Normalize maps a raw Common Event payload to a TelemetryFrame.
// It is a pure function: no I/O, no state. The consume loop calls it and
// acts on the returned Outcome (ack, term, or nak).
func Normalize(data []byte, resolver pointlist.Resolver, gatewayID string) (*pb.TelemetryFrame, Outcome) {
	var evt common.Event
	if err := json.Unmarshal(data, &evt); err != nil {
		slog.Warn("normalizer: unmarshal error", "err", err)
		return nil, OutcomePoison
	}
	pointID, ok := resolver.Resolve(evt.ConnectorID, evt.LocalID)
	if !ok {
		slog.Warn("normalizer: unknown local_id", "connector", evt.ConnectorID, "local_id", evt.LocalID)
		return nil, OutcomeMiss
	}
	ts := evt.Timestamp
	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}
	return &pb.TelemetryFrame{
		GatewayId: gatewayID,
		PointId:   pointID,
		Value:     evt.Value,
		Timestamp: ts,
	}, OutcomeOK
}
