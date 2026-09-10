// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package normalizer

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	pb "nexus-gateway/gen"
	"nexus-gateway/internal/common"
	"nexus-gateway/internal/metrics"
	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/telemetry"
)

// Outcome classifies a Common Event so the consume loop can drop-and-meter
// poison and point-list-miss events distinctly (ADR-0002 best-effort).
type Outcome int

const (
	OutcomeOK     Outcome = iota // resolved → emit a TelemetryFrame
	OutcomePoison                // unparseable/permanently invalid → Term + meter
	OutcomeMiss                  // unknown local_id → Term + meter
)

// EventMsg is one fetched Common Event with its ack controls. It is the subset
// of jetstream.Msg the consume loop needs (which therefore satisfies it directly).
type EventMsg interface {
	Data() []byte
	Ack() error
	Term() error
	Nak() error
	InProgress() error
}

// EventSource is the seam over the durable JetStream pull consumer: it yields the
// next batch of Common Events as an iterator. Yielding (rather than returning a
// slice) preserves streaming — each message is processed as it arrives, not after
// the whole batch closes — which keeps ack/term latency low. JetStream is one
// adapter (jetstreamSource); tests inject an in-memory fake, so the consume loop
// is exercisable without NATS.
type EventSource interface {
	Fetch(max int, maxWait time.Duration) iter.Seq[EventMsg]
}

// MetadataResolver optionally enriches canonical point IDs for a specific sink.
type MetadataResolver interface {
	Resolve(pointID, protocol string) (*telemetry.DTDPFMetadata, bool)
}

// Normalizer is the single durable pull consumer on evt.> (ADR-0001, ADR-0005).
// It resolves native LocalID → canonical PointID via the resolver, then emits
// TelemetryFrames downstream. Unknown local_ids are skipped and metered.
type Normalizer struct {
	records chan telemetry.PendingRecord
}

// New wires the Normalizer to the live JetStream EVENTS stream (ADR-0005),
// creating the durable consumer and feeding the consume loop through a
// jetstreamSource adapter.
func New(ctx context.Context, js jetstream.JetStream, resolver pointlist.Resolver, gatewayID string) (*Normalizer, error) {
	return NewWithMetadata(ctx, js, resolver, nil, gatewayID)
}

// NewWithMetadata wires the Normalizer with optional sink-specific metadata enrichment.
func NewWithMetadata(ctx context.Context, js jetstream.JetStream, resolver pointlist.Resolver, metadata MetadataResolver, gatewayID string) (*Normalizer, error) {
	cons, err := js.CreateOrUpdateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable:       "normalizer",
		FilterSubject: "evt.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,
	})
	if err != nil {
		return nil, fmt.Errorf("create normalizer consumer: %w", err)
	}
	return NewWithSourceAndMetadata(ctx, jetstreamSource{cons: cons}, resolver, metadata, gatewayID), nil
}

// NewWithSource starts a Normalizer over an arbitrary EventSource. This is the
// testable seam; New is the production wrapper over JetStream.
func NewWithSource(ctx context.Context, src EventSource, resolver pointlist.Resolver, gatewayID string) *Normalizer {
	return NewWithSourceAndMetadata(ctx, src, resolver, nil, gatewayID)
}

// NewWithSourceAndMetadata is the testable constructor with optional enrichment.
func NewWithSourceAndMetadata(ctx context.Context, src EventSource, resolver pointlist.Resolver, metadata MetadataResolver, gatewayID string) *Normalizer {
	n := &Normalizer{records: make(chan telemetry.PendingRecord, 256)}
	go n.consume(ctx, src, resolver, metadata, gatewayID)
	return n
}

// Records returns normalized records whose source must be acknowledged after persistence.
func (n *Normalizer) Records() <-chan telemetry.PendingRecord {
	return n.records
}

func (n *Normalizer) consume(ctx context.Context, src EventSource, resolver pointlist.Resolver, metadata MetadataResolver, gatewayID string) {
	defer close(n.records)
	for {
		if ctx.Err() != nil {
			return
		}
		for msg := range src.Fetch(32, 500*time.Millisecond) {
			record, out := NormalizeRecord(msg.Data(), resolver, metadata, gatewayID)
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
			pending := telemetry.PendingRecord{Record: record, Ack: msg.Ack, Nak: msg.Nak, InProgress: msg.InProgress}
			ticker := time.NewTicker(5 * time.Second)
			enqueued := false
			for !enqueued {
				select {
				case n.records <- pending:
					enqueued = true
				case <-ticker.C:
					// The downstream pump can block for a long time under DTDPF's
					// BlockWhenFull backpressure; keep the JetStream ack deadline
					// alive instead of risking redelivery.
					_ = msg.InProgress()
				case <-ctx.Done():
					_ = msg.Nak()
					ticker.Stop()
					return
				}
			}
			ticker.Stop()
		}
	}
}

// jetstreamSource adapts a JetStream pull consumer to EventSource. It yields each
// fetched message as it streams off the batch; jetstream.Msg satisfies EventMsg.
type jetstreamSource struct {
	cons jetstream.Consumer
}

func (s jetstreamSource) Fetch(max int, maxWait time.Duration) iter.Seq[EventMsg] {
	return func(yield func(EventMsg) bool) {
		batch, err := s.cons.Fetch(max, jetstream.FetchMaxWait(maxWait))
		if err != nil {
			slog.Warn("normalizer: fetch error", "err", err)
			return
		}
		for m := range batch.Messages() {
			if !yield(m) {
				return
			}
		}
		if err := batch.Error(); err != nil {
			slog.Warn("normalizer: batch error", "err", err)
		}
	}
}

// Normalize maps a raw Common Event payload to a TelemetryFrame.
// It is a pure function: no I/O, no state. The consume loop calls it and
// acts on the returned Outcome (ack, term, or nak).
func Normalize(data []byte, resolver pointlist.Resolver, gatewayID string) (*pb.TelemetryFrame, Outcome) {
	record, outcome := NormalizeRecord(data, resolver, nil, gatewayID)
	if record == nil {
		return nil, outcome
	}
	return record.ToProto(), outcome
}

// NormalizeRecord maps a raw Common Event to the sink-independent telemetry record.
func NormalizeRecord(data []byte, resolver pointlist.Resolver, metadata MetadataResolver, gatewayID string) (*telemetry.Record, Outcome) {
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
	record := &telemetry.Record{
		EventID:   uuid.NewString(),
		GatewayID: gatewayID,
		PointID:   pointID,
		Value:     evt.Value,
		Timestamp: ts,
	}
	if metadata != nil {
		dtdpfMetadata, ok := metadata.Resolve(pointID, evt.Protocol)
		if !ok {
			slog.Warn("normalizer: DTDPF metadata unresolved", "point_id", pointID, "protocol", evt.Protocol)
			return nil, OutcomeMiss
		}
		record.DTDPF = dtdpfMetadata
	}
	return record, OutcomeOK
}
