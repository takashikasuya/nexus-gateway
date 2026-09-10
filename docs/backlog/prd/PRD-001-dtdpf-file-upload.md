# PRD-001: DTDPF file upload for oversized telemetry (contract ④)

**Epic:** [EP-012: DTDPF Event Hubs & File Upload Integration](../epic/EP-012-dtdpf-integration.md)
**Covers:** FEAT-048, FEAT-049, FEAT-050
**Status:** Draft
**Owner:** nexus-gateway core agent

## Problem

DTDPF contract ④ requires that a telemetry `values` payload larger than 1 KiB
(compact UTF-8 JSON) is not embedded in the Event Hubs body. Instead, the
payload is uploaded to GW Upload Storage and the Event Hubs message carries a
summary value plus `fileUpload`/`fileName`/`fileHash` reference properties.

Today the gateway's internal pipeline (`common.Event` → `telemetry.Record` →
SQLite outbox → `dtdpf.EncodeEvent` → Event Hubs) only carries a single
`float64` scalar value end-to-end. There is no path for an arbitrary JSON
`values` object, no storage client, and no durable orchestration that
guarantees "upload before notify" across a crash/restart.

This PRD scopes the **gateway-side (Go core)** work required to close that
gap. It intentionally does not scope connector-side (Python/Java/Go SDK)
changes to actually *emit* large payloads from the field — see
[Out of scope](#out-of-scope).

## Goals

1. The internal pipeline can carry an arbitrary `values` JSON object without
   breaking the existing scalar `value` path or the Building OS protobuf
   projection (FEAT-048).
2. A tested, swappable storage-client seam can PUT bytes to GW Upload Storage
   using a SAS URI, with retry, timeout, and a SHA-256 content hash (FEAT-049).
3. Attachment upload is durable and ordered before the Event Hubs
   notification; a crash between upload and notification does not duplicate
   the object or lose the notification; oversized (>10 MiB) payloads are
   skipped with a logged/metered condition, never chunked (FEAT-050).

## Non-Goals

- Connector SDK (Go/Python/Java) changes to produce non-scalar `values` from
  real field protocols — tracked separately, after this PRD's wire format
  lands, per EP-012's FEAT-048 note ("Define connector SDK compatibility for
  Go, Python, and Java before changing wire data").
- IoT Edge Binding A (local Blob auto-sync) — Binding B (direct PUT) only,
  per EP-012 non-goals.
- The GW連携API-driven SAS provisioning/refresh flow (FEAT-051) — this PRD
  takes the SAS URI from environment/secret configuration, static for the
  deployment, consistent with how `DTDPF_EVENTHUB_CONNECTION_STRING` is
  injected today.
- Entra ID credential support — the seam is interface-based so it can be
  added later without a caller-visible change; only SAS URI is implemented
  now (mirrors ADR-0008's existing SAS-based Event Hubs auth for consistency).
- Rule evaluation, IoT Hub Direct Methods — unchanged EP-012 non-goals.

## Design

### FEAT-048: structured telemetry values

- `internal/common.Event` gains an optional `Values map[string]json.RawMessage`
  (or `json.RawMessage` of the whole `values` object — see implementation
  note below) alongside the existing `Value float64`. When absent, behavior
  is unchanged (scalar-only, current tests keep passing).
- `internal/telemetry.Record` gains `Values json.RawMessage` (compact JSON of
  the `values` object, nil when the event is scalar-only). `Value float64`
  remains for the scalar/legacy path and Building OS protobuf projection
  (`ToProto`/`FromProto` untouched).
- `internal/storeforward.Buffer` schema gains a nullable `values_json` column;
  `WriteRecord`/`ReadBatch` persist and round-trip it. Existing rows
  (`values_json IS NULL`) continue to read back as scalar-only records — no
  migration/backfill needed beyond `ALTER TABLE ... ADD COLUMN` (nullable,
  no default-value backfill required for existing rows).
- `internal/normalizer.NormalizeRecord` copies `evt.Values` onto the record
  when present.
- `internal/dtdpf.EncodeEvent` computes the attachment threshold from
  `len(compact JSON of values)` per [C4-05] and continues to emit
  `values.value` as the summary regardless (FEAT-050 adds the upload branch).

### FEAT-049: GW Upload Storage client

- New package `internal/dtdpf/storage` (or `internal/storage`, decide during
  implementation) defines:
  ```go
  type Uploader interface {
      Put(ctx context.Context, objectName string, body []byte) error
  }
  ```
- `SASBlobUploader` implements `Uploader` using the Azure Blob SDK
  (`github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`) against a
  container SAS URI (`DTDPF_UPLOAD_SAS_URL`), with:
  - a bounded timeout per attempt (`DTDPF_UPLOAD_TIMEOUT`, default e.g. 30s),
  - retry with backoff on transient failures (reuse the existing
    `internal/retry` package if its policy shape fits),
  - `Content-MD5`/`Content-Type` set from the caller (JSON payloads).
- A `Hash(body []byte) string` helper returns the lowercase 64-char SHA-256
  hex digest per [C4-06].
- Object naming helper: `ObjectName(telemetryID, extension string) string`
  → `"{telemetryID}.{extension}"` per [C4-02].
- Unit tests use an in-memory fake `Uploader`/fake HTTP transport — no live
  Azure Storage call in the unit suite. A `live_test.go` opt-in test
  (`integration` build tag + `DTDPF_LIVE_TEST=true`), mirroring the existing
  `internal/dtdpf/live_test.go` pattern, is added for one manual real-storage
  verification.

### FEAT-050: durable attachment orchestration

- Outbox schema gains attachment-state columns (e.g.
  `attachment_state TEXT` ∈ `none|pending|uploaded`, nullable
  `attachment_file_name`, `attachment_file_hash`), so state survives a
  process restart.
- Orchestration order per record with `Values` exceeding the 1 KiB threshold
  and ≤ 10 MiB:
  1. Compute `fileName`/`fileHash`.
  2. If `attachment_state != uploaded`: call `Uploader.Put`, then persist
     `attachment_state = uploaded` in the same transaction pattern the outbox
     already uses (commit-before-ack).
  3. Only after `attachment_state == uploaded`, `dtdpf.EncodeEvent` adds
     `fileUpload=1`/`fileName`/`fileHash` properties and the sink sends the
     Event Hubs notification.
  4. The record is only considered delivered (cursor advances) after the
     Event Hubs send succeeds — matching the existing "cursor unchanged on
     failure, replay same persisted id/body" behavior in ADR-0008.
- Restart/crash semantics: if the process restarts with
  `attachment_state = uploaded` but the record not yet past the cursor, the
  upload step is skipped (idempotent — no re-upload) and only the Event Hubs
  notification is retried. If it restarts with `attachment_state = none` or
  `pending`, the upload is retried (uploading twice for the same
  `{telemetryID}.{ext}` object name is an accepted, harmless overwrite —
  content is identical because `telemetryID`/body do not change on replay).
- Payloads whose compact `values` JSON exceeds 10 MiB [C4-07]: no upload, no
  `fileUpload` properties, summary-only event sent; log at `warn` and
  increment a new `metrics` counter (mirrors existing
  `IncNormalizerInvalid`/`IncNormalizerUnresolved` patterns).
- Credentials/SAS URIs: environment-only, never logged; excluded from
  fixtures/tests per the epic's existing non-negotiable.

## Acceptance Criteria (traced to EP-012)

- [ ] `common.Event`/`telemetry.Record` carry an optional `values` JSON object
  without changing existing scalar tests or the Building OS protobuf shape.
- [ ] Threshold: compact UTF-8 JSON of `values` > 1,024 bytes triggers the
  upload path; exactly 1,024 bytes does not. Boundary-tested at 1,024/1,025.
- [ ] `SASBlobUploader.Put` uploads bytes to the configured SAS URL; unit
  tests cover success, transient-failure retry, and timeout, all against a
  fake transport (no live network in `go test`).
- [ ] `fileHash` is the lowercase 64-hex-char SHA-256 of the exact uploaded
  bytes; `fileName` is `{telemetryID}.{extension}`.
- [ ] Upload happens before the Event Hubs notification; a simulated failure
  between upload success and notification does not re-upload on retry
  (verified via a fake `Uploader` call-counter) and still delivers the
  notification with file properties on the next attempt.
- [ ] A payload > 10 MiB is never uploaded; the summary event is sent without
  file properties; a metric/log records the condition.
- [ ] `go build ./...` and `go test ./...` (excluding the opt-in `integration`
  tag) pass with no regression to existing DTDPF/normalizer/storeforward
  suites.

## Sequencing

1. FEAT-048 (Go core wire/record/outbox extension) — no external dependency,
   TDD from day one.
2. FEAT-049 (storage client) — parallel-safe with FEAT-048; needs a SAS URL
   for the opt-in live test only, not for unit tests.
3. FEAT-050 (orchestration) — depends on both FEAT-048 and FEAT-049 landing.

## Open Questions (deferred, not blocking this PRD)

Carried over from EP-012's "External Information Required" section — answers
refine FEAT-050/live-test details later but do not block the TDD unit-test
work above, which is written against the `Uploader` interface:

1. Exact GW Upload Storage account/container endpoint for live verification.
2. SAS provisioning/refresh mechanism (static secret vs. future GW
   integration API extension, FEAT-051).
3. Required object path beyond `{id}.{extension}`, and MIME type per
   telemetry type.
4. Whether cloud-side Event Hubs notification wait requires storage-service
   acknowledgment beyond a 2xx PUT response (assumed sufficient here).
