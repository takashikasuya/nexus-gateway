// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package storeforward

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"nexus-gateway/internal/telemetry"
)

// Pump writes records to the durable outbox before acknowledging their source messages.
func Pump(ctx context.Context, src <-chan telemetry.PendingRecord, buf *Buffer) {
	for {
		select {
		case pending, ok := <-src:
			if !ok {
				return
			}
			for {
				err := buf.WriteRecord(pending.Record)
				if err == nil {
					if pending.Ack != nil {
						if err := pending.Ack(); err != nil {
							slog.Warn("storeforward: source ack error", "err", err)
						}
					}
					break
				}
				if !errors.Is(err, ErrBufferFull) {
					slog.Warn("storeforward: buffer write error", "err", err)
					if pending.Nak != nil {
						_ = pending.Nak()
					}
					break
				}
				if pending.InProgress != nil {
					_ = pending.InProgress()
				}
				timer := time.NewTimer(10 * time.Second)
				select {
				case <-buf.SpaceNotify():
					timer.Stop()
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					if pending.Nak != nil {
						_ = pending.Nak()
					}
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
