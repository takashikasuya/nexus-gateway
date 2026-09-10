// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"nexus-gateway/internal/dtdpf/storage"
	"nexus-gateway/internal/metrics"
	"nexus-gateway/internal/telemetry"
)

// defaultAttachmentExtension is used for every DTDPF attachment object name
// today, since Values is always encoded as JSON ([C4-02]/[C4-03]).
const defaultAttachmentExtension = "json"

// AttachmentStore persists per-record upload state so a crash/restart does
// not re-upload an object that already succeeded, and so a retried
// notification reuses the same file name/hash (FEAT-050). storeforward.Buffer
// satisfies this interface.
type AttachmentStore interface {
	AttachmentState(eventID string) (state, fileName, fileHash string, err error)
	MarkAttachmentUploaded(eventID, fileName, fileHash string) error
}

// AttachmentOrchestrator implements AttachmentResolver: it decides whether a
// record's Values payload must be uploaded, uploads it durably before
// returning (so the caller can only send the Event Hubs notification
// afterward), and is idempotent across crash/restart via AttachmentStore.
type AttachmentOrchestrator struct {
	uploader  storage.Uploader
	store     AttachmentStore
	extension string
}

// NewAttachmentOrchestrator builds an AttachmentOrchestrator over uploader and
// store, both required.
func NewAttachmentOrchestrator(uploader storage.Uploader, store AttachmentStore) (*AttachmentOrchestrator, error) {
	if uploader == nil {
		return nil, errors.New("DTDPF attachment orchestrator requires a non-nil Uploader")
	}
	if store == nil {
		return nil, errors.New("DTDPF attachment orchestrator requires a non-nil AttachmentStore")
	}
	return &AttachmentOrchestrator{uploader: uploader, store: store, extension: defaultAttachmentExtension}, nil
}

// Resolve implements AttachmentResolver.
func (o *AttachmentOrchestrator) Resolve(ctx context.Context, record *telemetry.Record) (*Attachment, error) {
	compact, err := CompactValues(record)
	if err != nil {
		return nil, err
	}
	if compact == nil || len(compact) <= AttachmentThreshold {
		return nil, nil
	}
	if len(compact) > MaxAttachmentBytes {
		metrics.IncDTDPFAttachmentOversized()
		slog.Warn("dtdpf: values payload exceeds the 10 MiB attachment limit, sending summary only",
			"event_id", record.EventID, "bytes", len(compact))
		return nil, nil
	}

	fileName := storage.ObjectName(record.EventID, o.extension)
	fileHash := storage.Hash(compact)

	state, existingName, existingHash, err := o.store.AttachmentState(record.EventID)
	if err != nil {
		return nil, fmt.Errorf("read DTDPF attachment state for %s: %w", record.EventID, err)
	}
	if state == "uploaded" {
		// Already durable (crash-replay or a retried notification): reuse the
		// persisted name/hash rather than re-uploading or recomputing.
		return &Attachment{FileName: existingName, FileHash: existingHash}, nil
	}

	if err := o.uploader.Put(ctx, fileName, compact); err != nil {
		return nil, fmt.Errorf("upload DTDPF attachment for %s: %w", record.EventID, err)
	}
	if err := o.store.MarkAttachmentUploaded(record.EventID, fileName, fileHash); err != nil {
		return nil, fmt.Errorf("persist DTDPF attachment state for %s: %w", record.EventID, err)
	}
	return &Attachment{FileName: fileName, FileHash: fileHash}, nil
}
