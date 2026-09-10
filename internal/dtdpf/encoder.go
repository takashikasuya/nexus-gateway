// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"nexus-gateway/internal/telemetry"
)

// EncodedEvent is the transport-neutral representation of one Event Hubs event.
type EncodedEvent struct {
	Body          []byte
	Properties    map[string]any
	ContentType   string
	CorrelationID string
	PartitionKey  string
}

type eventBody struct {
	ID        string      `json:"id"`
	Type      *int        `json:"type,omitempty"`
	RootID    int64       `json:"rootId"`
	DTID      string      `json:"dtId"`
	Topic     string      `json:"topic"`
	EventTime string      `json:"eventTime"`
	Values    eventValues `json:"values"`
}

type eventValues struct {
	Value string `json:"value"`
}

// EncodeEvent converts a durable telemetry record to the DTDPF Event Hubs contract.
func EncodeEvent(record *telemetry.Record) (*EncodedEvent, error) {
	if record == nil {
		return nil, fmt.Errorf("encode nil telemetry record")
	}
	telemetryID, err := uuid.Parse(record.EventID)
	if err != nil || telemetryID.Version() != 4 {
		return nil, fmt.Errorf("invalid telemetry event id %q", record.EventID)
	}
	if record.DTDPF == nil || record.DTDPF.RootID <= 0 || record.DTDPF.DTID == "" || record.DTDPF.Topic == "" {
		return nil, fmt.Errorf("incomplete DTDPF metadata for point %q", record.PointID)
	}
	eventTime, err := time.Parse(time.RFC3339Nano, record.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid telemetry timestamp %q: %w", record.Timestamp, err)
	}

	rootID := strconv.FormatInt(record.DTDPF.RootID, 10)
	body, err := json.Marshal(eventBody{
		ID:        telemetryID.String(),
		Type:      record.DTDPF.Type,
		RootID:    record.DTDPF.RootID,
		DTID:      record.DTDPF.DTID,
		Topic:     record.DTDPF.Topic,
		EventTime: eventTime.UTC().Format(time.RFC3339Nano),
		Values:    eventValues{Value: strconv.FormatFloat(record.Value, 'g', -1, 64)},
	})
	if err != nil {
		return nil, fmt.Errorf("encode DTDPF event body: %w", err)
	}
	return &EncodedEvent{
		Body: body,
		Properties: map[string]any{
			"rootId": rootID,
			"dtId":   record.DTDPF.DTID,
		},
		ContentType:   "application/json",
		CorrelationID: telemetryID.String(),
		PartitionKey:  rootID,
	}, nil
}
