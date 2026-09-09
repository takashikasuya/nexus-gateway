// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/dtdpf"
	"nexus-gateway/internal/telemetry"
)

func TestEncodeEventMatchesDTDPFContract(t *testing.T) {
	pointType := 2
	record := &telemetry.Record{
		EventID: "43217568-443d-4b24-96d1-59887fdd1628", PointID: "R90_000001", Value: 256.3,
		Timestamp: "2021-07-01T10:23:45.1234567+09:00",
		DTDPF: &telemetry.DTDPFMetadata{
			RootID: 5, DTID: "R90_000001", Topic: "takenaka.co.jp/R90/west/2F/temp", Type: &pointType,
		},
	}

	event, err := dtdpf.EncodeEvent(record)
	require.NoError(t, err)
	assert.Equal(t, "application/json", event.ContentType)
	assert.Equal(t, record.EventID, event.CorrelationID)
	assert.Equal(t, "5", event.PartitionKey)
	assert.Equal(t, map[string]any{"rootId": "5", "dtId": "R90_000001"}, event.Properties)

	var body map[string]any
	require.NoError(t, json.Unmarshal(event.Body, &body))
	assert.Equal(t, record.EventID, body["id"])
	assert.Equal(t, float64(2), body["type"])
	assert.Equal(t, float64(5), body["rootId"], "rootId must be a JSON number")
	assert.Equal(t, "R90_000001", body["dtId"])
	assert.Equal(t, "takenaka.co.jp/R90/west/2F/temp", body["topic"])
	assert.Equal(t, "2021-07-01T01:23:45.1234567Z", body["eventTime"])
	assert.Equal(t, map[string]any{"value": "256.3"}, body["values"])
	assert.NotContains(t, body, "pointId")
	assert.NotContains(t, body, "sequenceNumber")
}

func TestEncodeEventOmitsUnsetType(t *testing.T) {
	record := &telemetry.Record{
		EventID: "43217568-443d-4b24-96d1-59887fdd1628", PointID: "R90_000001", Value: 1,
		Timestamp: "2021-07-01T01:23:45Z",
		DTDPF:     &telemetry.DTDPFMetadata{RootID: 5, DTID: "R90_000001", Topic: "topic"},
	}
	event, err := dtdpf.EncodeEvent(record)
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(event.Body, &body))
	assert.NotContains(t, body, "type")
}

func TestEncodeEventRejectsNonDurableIdentity(t *testing.T) {
	_, err := dtdpf.EncodeEvent(&telemetry.Record{})
	require.Error(t, err)
}

func TestEncodeEventCanonicalizesUUIDToLowercase(t *testing.T) {
	record := &telemetry.Record{
		EventID: "43217568-443D-4B24-96D1-59887FDD1628", PointID: "R90_000001", Value: 1,
		Timestamp: "2021-07-01T01:23:45Z",
		DTDPF:     &telemetry.DTDPFMetadata{RootID: 5, DTID: "R90_000001", Topic: "topic"},
	}
	event, err := dtdpf.EncodeEvent(record)
	require.NoError(t, err)
	assert.Equal(t, "43217568-443d-4b24-96d1-59887fdd1628", event.CorrelationID)
	assert.Contains(t, string(event.Body), `"id":"43217568-443d-4b24-96d1-59887fdd1628"`)
}
