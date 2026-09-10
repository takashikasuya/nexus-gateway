// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package storeforward

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	pb "nexus-gateway/gen"
	"nexus-gateway/internal/telemetry"
)

// ErrBufferFull signals backpressure without dropping an existing record.
var ErrBufferFull = errors.New("store-forward buffer full")

// OverflowPolicy controls whether a full outbox evicts or blocks.
type OverflowPolicy int

const (
	DropOldest OverflowPolicy = iota
	BlockWhenFull
)

// StoredFrame pairs a sequence number with a TelemetryFrame read from the buffer.
type StoredFrame struct {
	Seq    int64
	Frame  *pb.TelemetryFrame
	Record *telemetry.Record
}

// DTDPFMetadataResolver resolves legacy outbox rows by canonical point ID.
type DTDPFMetadataResolver interface {
	ResolvePoint(pointID string) (*telemetry.DTDPFMetadata, bool)
}

// Buffer is a bounded SQLite ring buffer (ADR-0002).
// On overflow it drops the oldest rows. Cursor tracks the last acked seq.
type Buffer struct {
	db       *sql.DB
	capacity int
	overflow OverflowPolicy

	mu     sync.Mutex
	drifts map[string]int64

	// Store-and-forward observability counters (ADR-0002). Atomic: written from
	// the pump goroutine (written/dropped) and the uplink Forwarder goroutine
	// (sent/checkpoints/sendErrors), read from the Admin API handler goroutine.
	written     atomic.Int64
	dropped     atomic.Int64
	sent        atomic.Int64
	checkpoints atomic.Int64
	sendErrors  atomic.Int64

	// notify is signaled (non-blocking, coalesced) after each successful Write so
	// the single uplink Forwarder can drain immediately instead of polling (#71).
	notify chan struct{}
	space  chan struct{}
}

// Open opens (or creates) a Buffer at the given file path with the given capacity.
//
// The pump (Write) and the uplink Forwarder (Advance) both write, so they
// contend for SQLite's single writer. To avoid SQLITE_BUSY 'database is locked'
// under high write rates (#109) we cap the pool at a single connection so all
// access is serialized at the Go layer instead of racing across connections,
// and set WAL + synchronous=NORMAL + busy_timeout on that connection (the
// timeout is a hedge in case the cap is ever raised). Pragmas are applied via
// Exec rather than a `file:` DSN so the raw path is honored verbatim (paths
// containing URI metacharacters like '#', '?', '%', or ':memory:' would be
// misparsed as a URI).
func Open(path string, capacity int) (*Buffer, error) {
	return OpenWithPolicy(path, capacity, DropOldest)
}

// OpenWithPolicy opens a Buffer with an explicit overflow policy.
func OpenWithPolicy(path string, capacity int, overflow OverflowPolicy) (*Buffer, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	// One connection: the pump and Forwarder queue on it rather than colliding
	// on the SQLite writer lock. Set before any Exec so the pragmas below land
	// on the single connection the pool will reuse.
	db.SetMaxOpenConns(1)
	synchronousMode := "NORMAL"
	if overflow == BlockWhenFull {
		synchronousMode = "FULL"
	}
	for _, pragma := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=` + synchronousMode,
		`PRAGMA busy_timeout=5000`,
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close() //nolint:errcheck
			return nil, fmt.Errorf("sqlite %s: %w", pragma, err)
		}
	}
	if err := migrate(db); err != nil {
		db.Close() //nolint:errcheck
		return nil, err
	}
	return &Buffer{
		db: db, capacity: capacity, overflow: overflow, drifts: make(map[string]int64),
		notify: make(chan struct{}, 1), space: make(chan struct{}, 1),
	}, nil
}

// WriteNotify returns a channel signaled (coalesced to one pending slot) after
// each successful Write. The single uplink consumer selects on it to drain
// promptly; missed signals are covered by the consumer's own backstop tick.
func (b *Buffer) WriteNotify() <-chan struct{} { return b.notify }

// SpaceNotify is signaled after committed rows are removed in blocking mode.
func (b *Buffer) SpaceNotify() <-chan struct{} { return b.space }

// Close closes the underlying database.
func (b *Buffer) Close() error {
	return b.db.Close()
}

// PrepareDTDPF backfills legacy uncommitted rows once so replay keeps a stable ID and body.
func (b *Buffer) PrepareDTDPF(resolver DTDPFMetadataResolver) (int, error) {
	if resolver == nil {
		return 0, errors.New("prepare DTDPF requires a metadata resolver")
	}
	tx, err := b.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.Query(`
		SELECT seq, event_id, point_id
		FROM frames
		WHERE seq > COALESCE((SELECT seq FROM cursor WHERE id = 1), 0)
		  AND (event_id = '' OR dtdpf_root_id IS NULL OR dtdpf_dt_id IS NULL OR dtdpf_topic IS NULL)
		ORDER BY seq ASC`)
	if err != nil {
		return 0, err
	}
	type legacyRow struct {
		seq              int64
		eventID, pointID string
	}
	var pending []legacyRow
	for rows.Next() {
		var row legacyRow
		if err := rows.Scan(&row.seq, &row.eventID, &row.pointID); err != nil {
			rows.Close()
			return 0, err
		}
		pending = append(pending, row)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}

	for _, row := range pending {
		metadata, ok := resolver.ResolvePoint(row.pointID)
		if !ok {
			return 0, fmt.Errorf("legacy outbox point %q has no DTDPF metadata", row.pointID)
		}
		eventID := row.eventID
		if eventID == "" {
			eventID = uuid.NewString()
		}
		var pointType any
		if metadata.Type != nil {
			pointType = *metadata.Type
		}
		if _, err := tx.Exec(`
			UPDATE frames SET event_id = ?, dtdpf_root_id = ?, dtdpf_dt_id = ?,
				dtdpf_topic = ?, dtdpf_type = ?, dtdpf_protocol = ?
			WHERE seq = ?`,
			eventID, metadata.RootID, metadata.DTID, metadata.Topic, pointType, metadata.Protocol, row.seq,
		); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(pending), nil
}

// Write appends a frame. If the buffer is at capacity, the oldest row is deleted first.
func (b *Buffer) Write(f *pb.TelemetryFrame) error {
	return b.WriteRecord(telemetry.FromProto(f))
}

// WriteRecord appends a sink-independent telemetry record to the durable outbox.
func (b *Buffer) WriteRecord(record *telemetry.Record) error {
	if record == nil {
		return errors.New("write nil telemetry record")
	}
	attributes, err := json.Marshal(record.Attributes)
	if err != nil {
		return fmt.Errorf("marshal telemetry attributes: %w", err)
	}
	var rootID, dtdpfType any
	var dtID, topic, protocol any
	if record.DTDPF != nil {
		rootID = record.DTDPF.RootID
		dtID = record.DTDPF.DTID
		topic = record.DTDPF.Topic
		protocol = record.DTDPF.Protocol
		if record.DTDPF.Type != nil {
			dtdpfType = *record.DTDPF.Type
		}
	}
	var valuesJSON any
	if record.Values != nil {
		compacted, err := compactJSON(record.Values)
		if err != nil {
			return fmt.Errorf("compact telemetry values: %w", err)
		}
		valuesJSON = string(compacted)
	}
	attachmentEligible := 0
	if record.AttachmentEligible {
		attachmentEligible = 1
	}

	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if b.overflow == BlockWhenFull {
		var count int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM frames`).Scan(&count); err != nil {
			return err
		}
		if count >= b.capacity {
			return ErrBufferFull
		}
	}

	_, err = tx.Exec(
		`INSERT INTO frames (
			event_id, gateway_id, point_id, value, timestamp, attributes_json,
			dtdpf_root_id, dtdpf_dt_id, dtdpf_topic, dtdpf_type, dtdpf_protocol, values_json, attachment_eligible
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.EventID, record.GatewayID, record.PointID, record.Value, record.Timestamp, string(attributes),
		rootID, dtID, topic, dtdpfType, protocol, valuesJSON, attachmentEligible,
	)
	if err != nil {
		return err
	}

	var evicted int64
	if b.overflow == DropOldest {
		res, err := tx.Exec(`
			DELETE FROM frames
			WHERE seq IN (
				SELECT seq FROM frames ORDER BY seq ASC LIMIT MAX(0, (SELECT COUNT(*) FROM frames) - ?)
			)`, b.capacity)
		if err != nil {
			return err
		}
		evicted, _ = res.RowsAffected()
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	b.written.Add(1)
	if evicted > 0 {
		b.dropped.Add(evicted)
	}
	if b.notify != nil {
		select {
		case b.notify <- struct{}{}:
		default: // a signal is already pending; coalesce
		}
	}
	return nil
}

// RecordSent adds n to the count of frames acked-as-sent to Building OS.
func (b *Buffer) RecordSent(n int64) { b.sent.Add(n) }

// RecordCheckpoint counts one successful ack-checkpoint (ADR-0002).
func (b *Buffer) RecordCheckpoint() { b.checkpoints.Add(1) }

// RecordSendError counts one uplink send/checkpoint failure.
func (b *Buffer) RecordSendError() { b.sendErrors.Add(1) }

// Written returns the total frames successfully written to the buffer.
func (b *Buffer) Written() int64 { return b.written.Load() }

// Dropped returns the total frames evicted by drop-oldest at capacity (ADR-0002).
func (b *Buffer) Dropped() int64 { return b.dropped.Load() }

// Sent returns the total frames acked-as-sent to Building OS.
func (b *Buffer) Sent() int64 { return b.sent.Load() }

// Checkpoints returns the total successful ack-checkpoints.
func (b *Buffer) Checkpoints() int64 { return b.checkpoints.Load() }

// SendErrors returns the total uplink send/checkpoint failures.
func (b *Buffer) SendErrors() int64 { return b.sendErrors.Load() }

// AttachmentState returns the persisted DTDPF contract ④ upload state for a
// record's stable event ID (FEAT-050): state is one of "none", "pending", or
// "uploaded". An eventID with no matching row (not yet written, or a
// different buffer) returns "none" with no error — the caller should treat
// that identically to a fresh, never-uploaded record.
func (b *Buffer) AttachmentState(eventID string) (state, fileName, fileHash string, err error) {
	err = b.db.QueryRow(
		`SELECT attachment_state, COALESCE(attachment_file_name, ''), COALESCE(attachment_file_hash, '')
		 FROM frames WHERE event_id = ?`, eventID,
	).Scan(&state, &fileName, &fileHash)
	if errors.Is(err, sql.ErrNoRows) {
		return "none", "", "", nil
	}
	if err != nil {
		return "", "", "", err
	}
	return state, fileName, fileHash, nil
}

// MarkAttachmentUploaded records that eventID's oversized Values payload has
// been durably uploaded as fileName with the given content hash, so a
// restart or retry does not re-upload it (FEAT-050). Idempotent: calling it
// again with the same eventID/fileName/fileHash is a harmless no-op. Returns
// an error if eventID does not match exactly one row, so a silently-missed
// persist (or an unexpected multi-row update) is never mistaken for success.
func (b *Buffer) MarkAttachmentUploaded(eventID, fileName, fileHash string) error {
	res, err := b.db.Exec(
		`UPDATE frames SET attachment_state = 'uploaded', attachment_file_name = ?, attachment_file_hash = ?
		 WHERE event_id = ?`, fileName, fileHash, eventID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("mark DTDPF attachment uploaded: expected exactly 1 row for event_id %q, affected %d", eventID, affected)
	}
	return nil
}

// ReadBatch returns up to limit frames with seq > afterSeq, in ascending order.
func (b *Buffer) ReadBatch(afterSeq int64, limit int) ([]StoredFrame, error) {
	rows, err := b.db.Query(
		`SELECT seq, event_id, gateway_id, point_id, value, timestamp, attributes_json,
			dtdpf_root_id, dtdpf_dt_id, dtdpf_topic, dtdpf_type, dtdpf_protocol, values_json, attachment_eligible
		 FROM frames WHERE seq > ? ORDER BY seq ASC LIMIT ?`,
		afterSeq, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batch []StoredFrame
	for rows.Next() {
		var sf StoredFrame
		var attributesJSON string
		var rootID, dtdpfType sql.NullInt64
		var dtID, topic, protocol, valuesJSON sql.NullString
		var attachmentEligible int
		sf.Record = &telemetry.Record{}
		if err := rows.Scan(
			&sf.Seq, &sf.Record.EventID, &sf.Record.GatewayID, &sf.Record.PointID,
			&sf.Record.Value, &sf.Record.Timestamp, &attributesJSON,
			&rootID, &dtID, &topic, &dtdpfType, &protocol, &valuesJSON, &attachmentEligible,
		); err != nil {
			return nil, err
		}
		sf.Record.AttachmentEligible = attachmentEligible != 0
		if valuesJSON.Valid {
			sf.Record.Values = json.RawMessage(valuesJSON.String)
		}
		if err := json.Unmarshal([]byte(attributesJSON), &sf.Record.Attributes); err != nil {
			return nil, fmt.Errorf("decode telemetry attributes at seq %d: %w", sf.Seq, err)
		}
		if rootID.Valid {
			eventID, err := uuid.Parse(sf.Record.EventID)
			if err != nil || eventID.Version() != 4 {
				return nil, fmt.Errorf("invalid DTDPF event id at seq %d", sf.Seq)
			}
			sf.Record.DTDPF = &telemetry.DTDPFMetadata{
				RootID: rootID.Int64, DTID: dtID.String, Topic: topic.String, Protocol: protocol.String,
			}
			if dtdpfType.Valid {
				pointType := int(dtdpfType.Int64)
				sf.Record.DTDPF.Type = &pointType
			}
		}
		sf.Frame = sf.Record.ToProto()
		batch = append(batch, sf)
	}
	return batch, rows.Err()
}

// Advance persists the cursor to seq. Future ReadBatch calls with afterSeq=cursor skip delivered frames.
func (b *Buffer) Advance(seq int64) error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`INSERT OR REPLACE INTO cursor (id, seq) VALUES (1, ?)`, seq); err != nil {
		return err
	}
	if b.overflow == BlockWhenFull {
		if _, err := tx.Exec(`DELETE FROM frames WHERE seq <= ?`, seq); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if b.overflow == BlockWhenFull {
		select {
		case b.space <- struct{}{}:
		default:
		}
	}
	return nil
}

// Cursor returns the current persisted cursor (last acked seq).
// Returns 0 for a fresh buffer (no cursor row yet). Logs a warning for any other error.
func (b *Buffer) Cursor() int64 {
	var seq int64
	err := b.db.QueryRow(`SELECT seq FROM cursor WHERE id = 1`).Scan(&seq)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		slog.Warn("storeforward: cursor read error", "err", err)
	}
	return seq
}

// Depth returns the un-forwarded backlog: frames with seq beyond the cursor.
// Rows are retained after ack (only dropped on capacity overflow), so a plain
// COUNT(*) would track written_total rather than the real send backlog (#109).
func (b *Buffer) Depth() int64 {
	var n int64
	if err := b.db.QueryRow(
		`SELECT COUNT(*) FROM frames WHERE seq > COALESCE((SELECT seq FROM cursor WHERE id = 1), 0)`,
	).Scan(&n); err != nil {
		slog.Warn("storeforward: depth query error", "err", err)
	}
	return n
}

// RecordDrift increments the in-memory drift counter for pointID by delta.
func (b *Buffer) RecordDrift(pointID string, delta int64) {
	b.mu.Lock()
	b.drifts[pointID] += delta
	b.mu.Unlock()
}

// Drifts returns a snapshot of per-point_id drift counters.
func (b *Buffer) Drifts() map[string]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]int64, len(b.drifts))
	for k, v := range b.drifts {
		out[k] = v
	}
	return out
}

func migrate(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS frames (
			seq        INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id   TEXT NOT NULL DEFAULT '',
			gateway_id TEXT NOT NULL DEFAULT '',
			point_id   TEXT NOT NULL,
			value      REAL NOT NULL,
			timestamp  TEXT NOT NULL,
			attributes_json TEXT NOT NULL DEFAULT '{}',
			dtdpf_root_id INTEGER,
			dtdpf_dt_id TEXT,
			dtdpf_topic TEXT,
			dtdpf_type INTEGER,
			dtdpf_protocol TEXT
		);
		CREATE TABLE IF NOT EXISTS cursor (
			id  INTEGER PRIMARY KEY CHECK (id = 1),
			seq INTEGER NOT NULL DEFAULT 0
		);
	`); err != nil {
		return err
	}

	columns := []struct {
		name       string
		definition string
	}{
		{"event_id", "TEXT NOT NULL DEFAULT ''"},
		{"attributes_json", "TEXT NOT NULL DEFAULT '{}'"},
		{"dtdpf_root_id", "INTEGER"},
		{"dtdpf_dt_id", "TEXT"},
		{"dtdpf_topic", "TEXT"},
		{"dtdpf_type", "INTEGER"},
		{"dtdpf_protocol", "TEXT"},
		{"values_json", "TEXT"},
		{"attachment_state", "TEXT NOT NULL DEFAULT 'none'"},
		{"attachment_file_name", "TEXT"},
		{"attachment_file_hash", "TEXT"},
		{"attachment_eligible", "INTEGER NOT NULL DEFAULT 0"},
	}
	for _, column := range columns {
		exists, err := columnExists(db, "frames", column.name)
		if err != nil {
			return err
		}
		if !exists {
			if _, err := db.Exec(fmt.Sprintf("ALTER TABLE frames ADD COLUMN %s %s", column.name, column.definition)); err != nil {
				return fmt.Errorf("add frames.%s: %w", column.name, err)
			}
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_frames_event_id ON frames(event_id)`); err != nil {
		return fmt.Errorf("create frames.event_id index: %w", err)
	}
	return nil
}

// compactJSON returns the compact (no whitespace) encoding of a JSON value.
// Persisted values_json therefore normalizes away the caller's original
// whitespace/formatting; it is not a byte-for-byte copy of the input.
func compactJSON(raw json.RawMessage) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}
