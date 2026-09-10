// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/coder/websocket"

	"nexus-gateway/internal/telemetry"
)

// EventHubsTransport selects the AMQP network binding.
type EventHubsTransport string

const (
	TransportAMQPTCP   EventHubsTransport = "amqp-tcp"
	TransportWebSocket EventHubsTransport = "websocket"
)

type eventBatch interface {
	AddEvent(event *azeventhubs.EventData) error
	Len() int
}

type eventProducer interface {
	NewBatch(ctx context.Context, partitionKey string) (eventBatch, error)
	SendBatch(ctx context.Context, batch eventBatch) error
	Close(ctx context.Context) error
}

// AttachmentResolver decides whether a record's Values payload must be
// durably uploaded before its Event Hubs notification is sent (DTDPF
// contract ④, FEAT-050), returning the reference properties to attach once
// upload succeeds. It returns (nil, nil) when no attachment applies (absent,
// under threshold, or over the 10 MiB limit).
type AttachmentResolver interface {
	Resolve(ctx context.Context, record *telemetry.Record) (*Attachment, error)
}

// EventHubsSink batches DTDPF events by rootId and sends them in source order.
type EventHubsSink struct {
	producer    eventProducer
	groups      map[string][]*azeventhubs.EventData
	keyOrder    []string
	attachments AttachmentResolver
}

// WithAttachments enables DTDPF contract ④ attachment orchestration for this
// sink; call once after construction, before the first Send.
func (s *EventHubsSink) WithAttachments(resolver AttachmentResolver) *EventHubsSink {
	s.attachments = resolver
	return s
}

// NewEventHubsSink creates a DTDPF sink using an Event Hubs SAS connection string.
func NewEventHubsSink(connectionString, eventHub string) (*EventHubsSink, error) {
	return NewEventHubsSinkWithTransport(connectionString, eventHub, TransportAMQPTCP)
}

// NewEventHubsSinkWithTransport creates a DTDPF sink using the selected AMQP binding.
func NewEventHubsSinkWithTransport(connectionString, eventHub string, transport EventHubsTransport) (*EventHubsSink, error) {
	if strings.TrimSpace(connectionString) == "" {
		return nil, fmt.Errorf("DTDPF Event Hubs connection string is required")
	}
	if strings.TrimSpace(eventHub) == "" {
		return nil, fmt.Errorf("DTDPF Event Hub name is required")
	}
	sdkEventHub, err := eventHubSDKArgument(connectionString, eventHub)
	if err != nil {
		return nil, err
	}
	options := &azeventhubs.ProducerClientOptions{}
	switch transport {
	case TransportAMQPTCP:
	case TransportWebSocket:
		options.NewWebSocketConn = newWebSocketConn
	default:
		return nil, fmt.Errorf("unsupported DTDPF Event Hubs transport %q", transport)
	}
	client, err := azeventhubs.NewProducerClientFromConnectionString(connectionString, sdkEventHub, options)
	if err != nil {
		// Wrap the SDK error (it does not echo the raw connection string) so
		// operators can distinguish misconfiguration from transient failures.
		return nil, fmt.Errorf("create DTDPF Event Hubs producer: %w", err)
	}
	return newEventHubsSink(&azureProducer{client: client}), nil
}

func newWebSocketConn(ctx context.Context, params azeventhubs.WebSocketConnParams) (net.Conn, error) {
	connection, _, err := websocket.Dial(ctx, params.Host, &websocket.DialOptions{Subprotocols: []string{"amqp"}})
	if err != nil {
		return nil, err
	}
	return websocket.NetConn(ctx, connection, websocket.MessageBinary), nil
}

func eventHubSDKArgument(connectionString, configuredEventHub string) (string, error) {
	for part := range strings.SplitSeq(connectionString, ";") {
		key, value, found := strings.Cut(part, "=")
		if !found || !strings.EqualFold(strings.TrimSpace(key), "EntityPath") {
			continue
		}
		entityPath := strings.TrimSpace(value)
		if entityPath == "" {
			return "", fmt.Errorf("DTDPF Event Hubs connection string has an empty EntityPath")
		}
		if !strings.EqualFold(entityPath, strings.TrimSpace(configuredEventHub)) {
			return "", fmt.Errorf("DTDPF Event Hub name does not match connection string EntityPath")
		}
		return "", nil
	}
	return configuredEventHub, nil
}

func newEventHubsSink(producer eventProducer) *EventHubsSink {
	return &EventHubsSink{producer: producer, groups: make(map[string][]*azeventhubs.EventData)}
}

// Send prepares an event for the next checkpoint without performing network I/O.
func (s *EventHubsSink) Send(ctx context.Context, record *telemetry.Record) error {
	var attachment *Attachment
	if s.attachments != nil {
		var err error
		attachment, err = s.attachments.Resolve(ctx, record)
		if err != nil {
			return fmt.Errorf("resolve DTDPF attachment: %w", err)
		}
	}
	encoded, err := EncodeEvent(record, attachment)
	if err != nil {
		return err
	}
	contentType := encoded.ContentType
	event := &azeventhubs.EventData{
		Body:          encoded.Body,
		Properties:    encoded.Properties,
		ContentType:   &contentType,
		CorrelationID: encoded.CorrelationID,
	}
	if _, exists := s.groups[encoded.PartitionKey]; !exists {
		s.keyOrder = append(s.keyOrder, encoded.PartitionKey)
	}
	s.groups[encoded.PartitionKey] = append(s.groups[encoded.PartitionKey], event)
	return nil
}

// Checkpoint sends all pending rootId batches. A partial send followed by an
// error returns zero accepted so the outbox replays every event with the same ID.
func (s *EventHubsSink) Checkpoint(ctx context.Context) (accepted int64, err error) {
	defer s.reset()
	for _, partitionKey := range s.keyOrder {
		events := s.groups[partitionKey]
		batch, err := s.producer.NewBatch(ctx, partitionKey)
		if err != nil {
			return 0, fmt.Errorf("create Event Hubs batch for rootId %s: %w", partitionKey, err)
		}
		for _, event := range events {
			if err := batch.AddEvent(event); err != nil {
				if !errors.Is(err, azeventhubs.ErrEventDataTooLarge) {
					return 0, fmt.Errorf("add Event Hubs event for rootId %s: %w", partitionKey, err)
				}
				if batch.Len() == 0 {
					return 0, fmt.Errorf("DTDPF event exceeds Event Hubs message limit for rootId %s: %w", partitionKey, err)
				}
				if err := s.producer.SendBatch(ctx, batch); err != nil {
					return 0, fmt.Errorf("send Event Hubs batch for rootId %s: %w", partitionKey, err)
				}
				batch, err = s.producer.NewBatch(ctx, partitionKey)
				if err != nil {
					return 0, fmt.Errorf("create split Event Hubs batch for rootId %s: %w", partitionKey, err)
				}
				if err := batch.AddEvent(event); err != nil {
					return 0, fmt.Errorf("add Event Hubs event to empty batch for rootId %s: %w", partitionKey, err)
				}
			}
		}
		if batch.Len() > 0 {
			if err := s.producer.SendBatch(ctx, batch); err != nil {
				return 0, fmt.Errorf("send Event Hubs batch for rootId %s: %w", partitionKey, err)
			}
		}
		accepted += int64(len(events))
	}
	return accepted, nil
}

// Close releases the Event Hubs producer.
func (s *EventHubsSink) Close(ctx context.Context) error {
	return s.producer.Close(ctx)
}

func (s *EventHubsSink) reset() {
	clear(s.groups)
	s.keyOrder = s.keyOrder[:0]
}

type azureProducer struct {
	client *azeventhubs.ProducerClient
}

func (p *azureProducer) NewBatch(ctx context.Context, partitionKey string) (eventBatch, error) {
	batch, err := p.client.NewEventDataBatch(ctx, &azeventhubs.EventDataBatchOptions{PartitionKey: &partitionKey})
	if err != nil {
		return nil, err
	}
	return &azureBatch{batch: batch}, nil
}

func (p *azureProducer) SendBatch(ctx context.Context, batch eventBatch) error {
	azure, ok := batch.(*azureBatch)
	if !ok {
		return fmt.Errorf("unexpected Event Hubs batch type %T", batch)
	}
	return p.client.SendEventDataBatch(ctx, azure.batch, nil)
}

func (p *azureProducer) Close(ctx context.Context) error {
	return p.client.Close(ctx)
}

type azureBatch struct {
	batch *azeventhubs.EventDataBatch
}

func (b *azureBatch) AddEvent(event *azeventhubs.EventData) error {
	return b.batch.AddEventData(event, nil)
}

func (b *azureBatch) Len() int {
	return int(b.batch.NumEvents())
}
