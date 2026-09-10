// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"encoding/json"

	pb "nexus-gateway/gen"
)

// DTDPFMetadata carries the DTDPF routing data resolved from pointConfig.json.
type DTDPFMetadata struct {
	RootID   int64
	DTID     string
	Topic    string
	Type     *int
	Protocol string
}

// Record is the sink-independent telemetry value persisted in the durable outbox.
type Record struct {
	EventID    string
	GatewayID  string
	PointID    string
	Value      float64
	Timestamp  string
	Attributes map[string]string
	DTDPF      *DTDPFMetadata
	// Values optionally carries a typed JSON object alongside the scalar
	// Value (DTDPF contract ④, FEAT-048). It is copied through as received
	// (not guaranteed compact) and nil for the scalar-only path; the
	// Building OS protobuf projection (ToProto/FromProto) never carries it.
	Values json.RawMessage
}

// PendingRecord keeps the source acknowledgment attached until the record is durable.
type PendingRecord struct {
	Record     *Record
	Ack        func() error
	Nak        func() error
	InProgress func() error
}

// FromProto converts the public Building OS frame into the internal record.
func FromProto(frame *pb.TelemetryFrame) *Record {
	if frame == nil {
		return nil
	}
	return &Record{
		GatewayID:  frame.GatewayId,
		PointID:    frame.PointId,
		Value:      frame.Value,
		Timestamp:  frame.Timestamp,
		Attributes: cloneAttributes(frame.Attributes),
	}
}

// ToProto projects the internal record onto the stable Building OS contract.
func (r *Record) ToProto() *pb.TelemetryFrame {
	if r == nil {
		return nil
	}
	return &pb.TelemetryFrame{
		GatewayId:  r.GatewayID,
		PointId:    r.PointID,
		Value:      r.Value,
		Timestamp:  r.Timestamp,
		Attributes: cloneAttributes(r.Attributes),
	}
}

func cloneAttributes(attributes map[string]string) map[string]string {
	if attributes == nil {
		return nil
	}
	cloned := make(map[string]string, len(attributes))
	for key, value := range attributes {
		cloned[key] = value
	}
	return cloned
}
