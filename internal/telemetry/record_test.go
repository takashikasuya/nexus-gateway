// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package telemetry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "nexus-gateway/gen"
	"nexus-gateway/internal/telemetry"
)

func TestRecordProtoRoundTrip(t *testing.T) {
	frame := &pb.TelemetryFrame{
		GatewayId:  "gw-1",
		PointId:    "R90_000001",
		Value:      21.5,
		Timestamp:  "2026-09-09T01:02:03Z",
		Attributes: map[string]string{"quality": "good"},
	}

	record := telemetry.FromProto(frame)
	record.Attributes["quality"] = "changed"

	assert.Equal(t, "good", frame.Attributes["quality"], "conversion must not alias caller-owned attributes")
	assert.Equal(t, record.GatewayID, record.ToProto().GatewayId)
	assert.Equal(t, record.PointID, record.ToProto().PointId)
	assert.Equal(t, record.Value, record.ToProto().Value)
	assert.Equal(t, record.Timestamp, record.ToProto().Timestamp)
	assert.Equal(t, "changed", record.ToProto().Attributes["quality"])
}
