// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/dtdpf"
	"nexus-gateway/internal/metrics"
	"nexus-gateway/internal/telemetry"
)

// fakeUploader records every Put call for assertions and can be told to fail
// the next N calls, to simulate an upload-succeeds-but-notify-fails restart.
type fakeUploader struct {
	mu       sync.Mutex
	calls    []string
	failNext int
	failErr  error
}

func (f *fakeUploader) Put(_ context.Context, objectName string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return f.failErr
	}
	f.calls = append(f.calls, objectName)
	return nil
}

func (f *fakeUploader) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeAttachmentStore is an in-memory AttachmentStore, standing in for
// storeforward.Buffer so orchestration logic is tested without SQLite.
type fakeAttachmentStore struct {
	mu    sync.Mutex
	rows  map[string][3]string // eventID -> [state, fileName, fileHash]
	calls int
}

func newFakeAttachmentStore() *fakeAttachmentStore {
	return &fakeAttachmentStore{rows: make(map[string][3]string)}
}

func (s *fakeAttachmentStore) AttachmentState(eventID string) (state, fileName, fileHash string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[eventID]
	if !ok {
		return "none", "", "", nil
	}
	return row[0], row[1], row[2], nil
}

func (s *fakeAttachmentStore) MarkAttachmentUploaded(eventID, fileName, fileHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.rows[eventID] = [3]string{"uploaded", fileName, fileHash}
	return nil
}

func recordWithValues(eventID string, values []byte) *telemetry.Record {
	return &telemetry.Record{EventID: eventID, PointID: "p1", Timestamp: "2026-09-10T00:00:00Z", Values: values}
}

func largeValues(n int) []byte {
	const overhead = len(`{"v":""}`)
	return []byte(`{"v":"` + strings.Repeat("x", n-overhead) + `"}`)
}

func TestAttachmentOrchestrator_ResolveNoAttachmentUnderThreshold(t *testing.T) {
	uploader := &fakeUploader{}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	attachment, err := orch.Resolve(context.Background(), recordWithValues("id-1", []byte(`{"a":1}`)))
	require.NoError(t, err)
	assert.Nil(t, attachment)
	assert.Equal(t, 0, uploader.callCount(), "an under-threshold payload must never be uploaded")
}

func TestAttachmentOrchestrator_ResolveNoAttachmentWhenValuesAbsent(t *testing.T) {
	uploader := &fakeUploader{}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	attachment, err := orch.Resolve(context.Background(), &telemetry.Record{EventID: "id-1"})
	require.NoError(t, err)
	assert.Nil(t, attachment)
}

func TestAttachmentOrchestrator_UploadsBeforeReturningAttachment(t *testing.T) {
	uploader := &fakeUploader{}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	record := recordWithValues("43217568-443d-4b24-96d1-59887fdd1628", largeValues(dtdpf.AttachmentThreshold+1))
	attachment, err := orch.Resolve(context.Background(), record)
	require.NoError(t, err)
	require.NotNil(t, attachment)
	assert.Equal(t, "43217568-443d-4b24-96d1-59887fdd1628.json", attachment.FileName)
	assert.NotEmpty(t, attachment.FileHash)
	assert.Equal(t, 1, uploader.callCount(), "the oversized payload must be uploaded exactly once")

	state, fileName, fileHash, err := store.AttachmentState(record.EventID)
	require.NoError(t, err)
	assert.Equal(t, "uploaded", state)
	assert.Equal(t, attachment.FileName, fileName)
	assert.Equal(t, attachment.FileHash, fileHash)
}

func TestAttachmentOrchestrator_ObjectNameUsesCanonicalLowercaseUUID(t *testing.T) {
	uploader := &fakeUploader{}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	// Uppercase EventID: EncodeEvent canonicalizes UUIDs to lowercase for the
	// body's "id" field, so the uploaded fileName must match that form too.
	record := recordWithValues("43217568-443D-4B24-96D1-59887FDD1628", largeValues(dtdpf.AttachmentThreshold+1))
	attachment, err := orch.Resolve(context.Background(), record)
	require.NoError(t, err)
	require.NotNil(t, attachment)
	assert.Equal(t, "43217568-443d-4b24-96d1-59887fdd1628.json", attachment.FileName)
}

func TestAttachmentOrchestrator_RejectsInvalidEventID(t *testing.T) {
	uploader := &fakeUploader{}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	record := recordWithValues("not-a-uuid", largeValues(dtdpf.AttachmentThreshold+1))
	_, err = orch.Resolve(context.Background(), record)
	require.Error(t, err)
	assert.Equal(t, 0, uploader.callCount())
}

func TestAttachmentOrchestrator_CrashReplayDoesNotReupload(t *testing.T) {
	uploader := &fakeUploader{}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	record := recordWithValues("43217568-443d-4b24-96d1-59887fdd1628", largeValues(dtdpf.AttachmentThreshold+1))

	first, err := orch.Resolve(context.Background(), record)
	require.NoError(t, err)

	// Simulate a restart: a fresh orchestrator instance over the same
	// (durable) store, resolving the same record again because the Event
	// Hubs notification for it never succeeded before the crash.
	restarted, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)
	second, err := restarted.Resolve(context.Background(), record)
	require.NoError(t, err)

	assert.Equal(t, first, second, "a replayed resolve must return the same persisted file name/hash")
	assert.Equal(t, 1, uploader.callCount(), "the upload must not be repeated once already durable")
}

func TestAttachmentOrchestrator_OversizedPayloadSkipsUploadAndMeters(t *testing.T) {
	before := metrics.DTDPFAttachmentOversized()
	uploader := &fakeUploader{}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	record := recordWithValues("id-1", largeValues(dtdpf.MaxAttachmentBytes+1))
	attachment, err := orch.Resolve(context.Background(), record)
	require.NoError(t, err)
	assert.Nil(t, attachment, "an over-10MiB payload must be sent as summary-only, no fileUpload properties")
	assert.Equal(t, 0, uploader.callCount(), "an over-10MiB payload must never be uploaded (never chunked)")
	assert.Equal(t, before+1, metrics.DTDPFAttachmentOversized())

	state, _, _, err := store.AttachmentState(record.EventID)
	require.NoError(t, err)
	assert.Equal(t, "none", state)
}

func TestAttachmentOrchestrator_UploadFailureLeavesStateUnuploaded(t *testing.T) {
	uploader := &fakeUploader{failNext: 1, failErr: errors.New("network error")}
	store := newFakeAttachmentStore()
	orch, err := dtdpf.NewAttachmentOrchestrator(uploader, store)
	require.NoError(t, err)

	record := recordWithValues("53217568-443d-4b24-96d1-59887fdd1628", largeValues(dtdpf.AttachmentThreshold+1))
	_, err = orch.Resolve(context.Background(), record)
	require.Error(t, err, "the caller must not send an Event Hubs notification when upload fails")

	state, _, _, stateErr := store.AttachmentState(record.EventID)
	require.NoError(t, stateErr)
	assert.Equal(t, "none", state, "a failed upload must not be marked uploaded")

	// Retry succeeds: the same record resolves normally on the next attempt.
	attachment, err := orch.Resolve(context.Background(), record)
	require.NoError(t, err)
	require.NotNil(t, attachment)
	assert.Equal(t, 1, uploader.callCount())
}

func TestAttachmentOrchestrator_RejectsNilDependencies(t *testing.T) {
	store := newFakeAttachmentStore()
	_, err := dtdpf.NewAttachmentOrchestrator(nil, store)
	require.Error(t, err)

	uploader := &fakeUploader{}
	_, err = dtdpf.NewAttachmentOrchestrator(uploader, nil)
	require.Error(t, err)
}

// jsonValues is a small helper asserting largeValues actually produces valid
// JSON of the requested length, guarding against a broken test fixture.
func TestLargeValuesFixtureIsValidJSONOfExactLength(t *testing.T) {
	raw := largeValues(2000)
	require.Len(t, raw, 2000)
	var v map[string]any
	require.NoError(t, json.Unmarshal(raw, &v))
}
