package storeforward

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	_ "modernc.org/sqlite"

	pb "nexus-gateway/gen"
)

// StoredFrame pairs a sequence number with a TelemetryFrame read from the buffer.
type StoredFrame struct {
	Seq   int64
	Frame *pb.TelemetryFrame
}

// Buffer is a bounded SQLite ring buffer (ADR-0002).
// On overflow it drops the oldest rows. Cursor tracks the last acked seq.
type Buffer struct {
	db       *sql.DB
	capacity int

	mu     sync.Mutex
	drifts map[string]int64
}

// Open opens (or creates) a Buffer at the given file path with the given capacity.
func Open(path string, capacity int) (*Buffer, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, err
	}
	if err := migrate(db); err != nil {
		return nil, err
	}
	return &Buffer{db: db, capacity: capacity, drifts: make(map[string]int64)}, nil
}

// Close closes the underlying database.
func (b *Buffer) Close() error {
	return b.db.Close()
}

// Write appends a frame. If the buffer is at capacity, the oldest row is deleted first.
func (b *Buffer) Write(f *pb.TelemetryFrame) error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	_, err = tx.Exec(
		`INSERT INTO frames (gateway_id, point_id, value, timestamp) VALUES (?, ?, ?, ?)`,
		f.GatewayId, f.PointId, f.Value, f.Timestamp,
	)
	if err != nil {
		return err
	}

	// Drop oldest if over capacity
	_, err = tx.Exec(`
		DELETE FROM frames
		WHERE seq IN (
			SELECT seq FROM frames ORDER BY seq ASC LIMIT MAX(0, (SELECT COUNT(*) FROM frames) - ?)
		)`, b.capacity)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// ReadBatch returns up to limit frames with seq > afterSeq, in ascending order.
func (b *Buffer) ReadBatch(afterSeq int64, limit int) ([]StoredFrame, error) {
	rows, err := b.db.Query(
		`SELECT seq, gateway_id, point_id, value, timestamp FROM frames WHERE seq > ? ORDER BY seq ASC LIMIT ?`,
		afterSeq, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batch []StoredFrame
	for rows.Next() {
		var sf StoredFrame
		sf.Frame = &pb.TelemetryFrame{}
		if err := rows.Scan(&sf.Seq, &sf.Frame.GatewayId, &sf.Frame.PointId, &sf.Frame.Value, &sf.Frame.Timestamp); err != nil {
			return nil, err
		}
		batch = append(batch, sf)
	}
	return batch, rows.Err()
}

// Advance persists the cursor to seq. Future ReadBatch calls with afterSeq=cursor skip delivered frames.
func (b *Buffer) Advance(seq int64) error {
	_, err := b.db.Exec(`INSERT OR REPLACE INTO cursor (id, seq) VALUES (1, ?)`, seq)
	return err
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

// Depth returns the number of frames currently stored in the buffer.
func (b *Buffer) Depth() int64 {
	var n int64
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM frames`).Scan(&n); err != nil {
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
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS frames (
			seq        INTEGER PRIMARY KEY AUTOINCREMENT,
			gateway_id TEXT NOT NULL DEFAULT '',
			point_id   TEXT NOT NULL,
			value      REAL NOT NULL,
			timestamp  TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS cursor (
			id  INTEGER PRIMARY KEY CHECK (id = 1),
			seq INTEGER NOT NULL DEFAULT 0
		);
	`)
	return err
}
