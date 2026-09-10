// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"nexus-gateway/internal/retry"
	"nexus-gateway/internal/storeforward"
	"nexus-gateway/internal/uplink"
)

// Uplink reconnects the Event Hubs producer and replays uncommitted outbox records.
type Uplink struct {
	connectionString string
	eventHub         string
	transport        EventHubsTransport
	buffer           *storeforward.Buffer
	config           uplink.Config
	initialSink      *EventHubsSink
}

// NewUplink validates Event Hubs configuration and prepares the first producer.
func NewUplink(connectionString, eventHub string, transport EventHubsTransport, buffer *storeforward.Buffer, config uplink.Config) (*Uplink, error) {
	if buffer == nil {
		return nil, fmt.Errorf("DTDPF uplink requires a store-forward buffer")
	}
	sink, err := NewEventHubsSinkWithTransport(connectionString, eventHub, transport)
	if err != nil {
		return nil, err
	}
	return &Uplink{
		connectionString: connectionString,
		eventHub:         eventHub,
		transport:        transport,
		buffer:           buffer,
		config:           config,
		initialSink:      sink,
	}, nil
}

// Run sends records until cancellation, recreating the producer after transport failures.
func (u *Uplink) Run(ctx context.Context) {
	sink := u.initialSink
	u.initialSink = nil
	backoff := &retry.Backoff{Min: time.Second, Max: 60 * time.Second, Factor: 2}

	for ctx.Err() == nil {
		if sink == nil {
			var err error
			sink, err = NewEventHubsSinkWithTransport(u.connectionString, u.eventHub, u.transport)
			if err != nil {
				slog.Warn("DTDPF Event Hubs producer recreation failed", "err", err)
				_ = backoff.Wait(ctx)
				continue
			}
		}

		err := uplink.NewForwarder(u.buffer, sink, u.config).Run(ctx)
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if closeErr := sink.Close(closeCtx); closeErr != nil && ctx.Err() == nil {
			slog.Warn("DTDPF Event Hubs producer close failed", "err", closeErr)
		}
		cancel()
		sink = nil
		if err != nil && ctx.Err() == nil {
			slog.Warn("DTDPF Event Hubs send failed, reconnecting", "err", err)
			_ = backoff.Wait(ctx)
		} else {
			backoff.Reset()
		}
	}
}
