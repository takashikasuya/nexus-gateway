// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"context"
	"encoding/json"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/common"
	"nexus-gateway/internal/normalizer"
	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/storeforward"
	"nexus-gateway/internal/uplink"
)

type pipelineMessage struct {
	data  []byte
	acked atomic.Bool
}

func (m *pipelineMessage) Data() []byte      { return m.data }
func (m *pipelineMessage) Ack() error        { m.acked.Store(true); return nil }
func (m *pipelineMessage) Term() error       { return nil }
func (m *pipelineMessage) Nak() error        { return nil }
func (m *pipelineMessage) InProgress() error { return nil }

type pipelineSource struct {
	mu      sync.Mutex
	message normalizer.EventMsg
}

func (s *pipelineSource) Fetch(_ int, _ time.Duration) iter.Seq[normalizer.EventMsg] {
	return func(yield func(normalizer.EventMsg) bool) {
		s.mu.Lock()
		message := s.message
		s.message = nil
		s.mu.Unlock()
		if message != nil {
			yield(message)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDTDPFTelemetryPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := LoadPointConfig(strings.NewReader(`{
		"pointData":{"5":[{
			"dtId":"R90_000001","topic":"takenaka.co.jp/R90/temp","type":2,"protocol":"bacnet"
		}]}
	}`))
	require.NoError(t, err)
	eventData, err := json.Marshal(common.Event{
		Protocol: "bacnet", ConnectorID: "bacnet-01", LocalID: "analogInput:1",
		Value: 21.5, Timestamp: "2026-09-09T01:02:03Z",
	})
	require.NoError(t, err)
	message := &pipelineMessage{data: eventData}
	source := &pipelineSource{message: message}
	points := pointlist.NewFixture([]pointlist.Entry{
		{ConnectorID: "bacnet-01", LocalID: "analogInput:1", PointID: "R90_000001"},
	})
	normalized := normalizer.NewWithSourceAndMetadata(ctx, source, points, config, "gw-1")

	buffer, err := storeforward.OpenWithPolicy(t.TempDir()+"/sf.db", 100, storeforward.BlockWhenFull)
	require.NoError(t, err)
	defer buffer.Close()
	go storeforward.Pump(ctx, normalized.Records(), buffer)

	producer := &fakeProducer{maxEvents: 100}
	forwarder := uplink.NewForwarder(buffer, newEventHubsSink(producer), uplink.Config{
		CheckpointSize: 1, CheckpointAge: time.Hour,
	})
	go func() { _ = forwarder.Run(ctx) }()

	assert.Eventually(t, func() bool {
		return message.acked.Load() && buffer.Cursor() > 0 && len(producer.sent) == 1
	}, 3*time.Second, 10*time.Millisecond)
	require.Len(t, producer.sent[0], 1)
	assert.Equal(t, "application/json", *producer.sent[0][0].ContentType)
	assert.Equal(t, "5", producer.sent[0][0].Properties["rootId"])
	assert.Contains(t, string(producer.sent[0][0].Body), `"dtId":"R90_000001"`)
}
