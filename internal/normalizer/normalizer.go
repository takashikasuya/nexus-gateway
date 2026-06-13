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
	"nexus-gateway/internal/pointlist"
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
			frame, ok := normalize(msg.Data(), resolver, gatewayID)
			if !ok {
				_ = msg.Nak()
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

func normalize(data []byte, resolver pointlist.Resolver, gatewayID string) (*pb.TelemetryFrame, bool) {
	var evt common.Event
	if err := json.Unmarshal(data, &evt); err != nil {
		slog.Warn("normalizer: unmarshal error", "err", err)
		return nil, false
	}
	pointID, ok := resolver.Resolve(evt.ConnectorID, evt.LocalID)
	if !ok {
		slog.Warn("normalizer: unknown local_id", "connector", evt.ConnectorID, "local_id", evt.LocalID)
		return nil, false
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
	}, true
}
