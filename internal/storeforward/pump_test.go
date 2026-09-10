// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package storeforward_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/storeforward"
	"nexus-gateway/internal/telemetry"
)

func TestPumpAcknowledgesOnlyAfterDurableWrite(t *testing.T) {
	buf, err := storeforward.OpenWithPolicy(t.TempDir()+"/sf.db", 1, storeforward.BlockWhenFull)
	require.NoError(t, err)
	defer buf.Close()
	require.NoError(t, buf.WriteRecord(&telemetry.Record{PointID: "existing", Timestamp: "t"}))

	source := make(chan telemetry.PendingRecord, 1)
	var acked, progressed atomic.Bool
	source <- telemetry.PendingRecord{
		Record:     &telemetry.Record{PointID: "pending", Timestamp: "t"},
		Ack:        func() error { acked.Store(true); return nil },
		InProgress: func() error { progressed.Store(true); return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go storeforward.Pump(ctx, source, buf)

	assert.Eventually(t, progressed.Load, time.Second, 10*time.Millisecond)
	assert.False(t, acked.Load(), "full outbox must not acknowledge the source")
	batch, err := buf.ReadBatch(0, 1)
	require.NoError(t, err)
	require.NoError(t, buf.Advance(batch[0].Seq))
	assert.Eventually(t, acked.Load, time.Second, 10*time.Millisecond)
}
