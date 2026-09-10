// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/telemetry"
)

type fakeProducer struct {
	maxEvents int
	keys      []string
	sent      [][]*azeventhubs.EventData
	sendErrAt int
}

func (p *fakeProducer) NewBatch(_ context.Context, partitionKey string) (eventBatch, error) {
	p.keys = append(p.keys, partitionKey)
	return &fakeBatch{maxEvents: p.maxEvents}, nil
}

func (p *fakeProducer) SendBatch(_ context.Context, batch eventBatch) error {
	if p.sendErrAt > 0 && len(p.sent)+1 == p.sendErrAt {
		return errors.New("send failed")
	}
	p.sent = append(p.sent, append([]*azeventhubs.EventData(nil), batch.(*fakeBatch).events...))
	return nil
}

func (p *fakeProducer) Close(context.Context) error { return nil }

type fakeBatch struct {
	maxEvents int
	events    []*azeventhubs.EventData
}

func (b *fakeBatch) AddEvent(event *azeventhubs.EventData) error {
	if b.maxEvents > 0 && len(b.events) >= b.maxEvents {
		return azeventhubs.ErrEventDataTooLarge
	}
	b.events = append(b.events, event)
	return nil
}

func (b *fakeBatch) Len() int { return len(b.events) }

func sinkRecord(id string, rootID int64, dtID string) *telemetry.Record {
	return &telemetry.Record{
		EventID: id, PointID: dtID, Value: 1, Timestamp: "2026-09-09T00:00:00Z",
		DTDPF: &telemetry.DTDPFMetadata{RootID: rootID, DTID: dtID, Topic: "topic/" + dtID},
	}
}

func TestEventHubsSinkGroupsAndSplitsByRootID(t *testing.T) {
	producer := &fakeProducer{maxEvents: 2}
	sink := newEventHubsSink(producer)
	records := []*telemetry.Record{
		sinkRecord("43217568-443d-4b24-96d1-59887fdd1628", 5, "p1"),
		sinkRecord("53217568-443d-4b24-96d1-59887fdd1628", 6, "p2"),
		sinkRecord("63217568-443d-4b24-96d1-59887fdd1628", 5, "p3"),
		sinkRecord("73217568-443d-4b24-96d1-59887fdd1628", 5, "p4"),
	}
	for _, record := range records {
		require.NoError(t, sink.Send(context.Background(), record))
	}

	accepted, err := sink.Checkpoint(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(4), accepted)
	assert.Equal(t, []string{"5", "5", "6"}, producer.keys)
	require.Len(t, producer.sent, 3)
	assert.Contains(t, string(producer.sent[0][0].Body), `"dtId":"p1"`)
	assert.Contains(t, string(producer.sent[0][1].Body), `"dtId":"p3"`)
	assert.Contains(t, string(producer.sent[1][0].Body), `"dtId":"p4"`)
	assert.Contains(t, string(producer.sent[2][0].Body), `"dtId":"p2"`)
}

func TestEventHubsSinkFailureDoesNotAcknowledgePartialSend(t *testing.T) {
	producer := &fakeProducer{maxEvents: 1, sendErrAt: 2}
	sink := newEventHubsSink(producer)
	require.NoError(t, sink.Send(context.Background(), sinkRecord("43217568-443d-4b24-96d1-59887fdd1628", 5, "p1")))
	require.NoError(t, sink.Send(context.Background(), sinkRecord("53217568-443d-4b24-96d1-59887fdd1628", 5, "p2")))

	accepted, err := sink.Checkpoint(context.Background())
	require.Error(t, err)
	assert.Zero(t, accepted)
	assert.Len(t, producer.sent, 1, "first batch may reach Event Hubs and will be duplicated on outbox replay")
}

func TestEventHubSDKArgument(t *testing.T) {
	withoutEntityPath := "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=secret="
	argument, err := eventHubSDKArgument(withoutEntityPath, "telemetry")
	require.NoError(t, err)
	assert.Equal(t, "telemetry", argument)

	withEntityPath := withoutEntityPath + ";EntityPath=testevthub"
	argument, err = eventHubSDKArgument(withEntityPath, "testevthub")
	require.NoError(t, err)
	assert.Empty(t, argument, "the SDK requires an empty eventHub argument when EntityPath is present")

	_, err = eventHubSDKArgument(withEntityPath, "different-hub")
	assert.Error(t, err)
}

type fakeAttachmentResolver struct {
	attachment *Attachment
	err        error
}

func (r fakeAttachmentResolver) Resolve(context.Context, *telemetry.Record) (*Attachment, error) {
	return r.attachment, r.err
}

func TestEventHubsSinkSendUsesAttachmentResolver(t *testing.T) {
	producer := &fakeProducer{}
	sink := newEventHubsSink(producer)
	sink.WithAttachments(fakeAttachmentResolver{attachment: &Attachment{FileName: "id.json", FileHash: "deadbeef"}})

	require.NoError(t, sink.Send(context.Background(), sinkRecord("43217568-443d-4b24-96d1-59887fdd1628", 5, "p1")))
	_, err := sink.Checkpoint(context.Background())
	require.NoError(t, err)

	require.Len(t, producer.sent, 1)
	require.Len(t, producer.sent[0], 1)
	assert.Equal(t, "1", producer.sent[0][0].Properties["fileUpload"])
	assert.Equal(t, "id.json", producer.sent[0][0].Properties["fileName"])
	assert.Equal(t, "deadbeef", producer.sent[0][0].Properties["fileHash"])
}

func TestEventHubsSinkSendPropagatesAttachmentResolverError(t *testing.T) {
	producer := &fakeProducer{}
	sink := newEventHubsSink(producer)
	sink.WithAttachments(fakeAttachmentResolver{err: errors.New("upload failed")})

	err := sink.Send(context.Background(), sinkRecord("43217568-443d-4b24-96d1-59887fdd1628", 5, "p1"))
	require.Error(t, err, "a failed attachment resolution must not enqueue the notification")

	accepted, err := sink.Checkpoint(context.Background())
	require.NoError(t, err)
	assert.Zero(t, accepted, "nothing should have been queued after the resolver error")
}

func TestEventHubsSinkSendWithoutAttachmentResolverOmitsFileProperties(t *testing.T) {
	producer := &fakeProducer{}
	sink := newEventHubsSink(producer)

	require.NoError(t, sink.Send(context.Background(), sinkRecord("43217568-443d-4b24-96d1-59887fdd1628", 5, "p1")))
	_, err := sink.Checkpoint(context.Background())
	require.NoError(t, err)

	require.Len(t, producer.sent, 1)
	require.Len(t, producer.sent[0], 1)
	assert.NotContains(t, producer.sent[0][0].Properties, "fileUpload")
}
