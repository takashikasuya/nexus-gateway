# EP-012: DTDPF Event Hubs & File Upload Integration

**Status:** In Progress — basic Event Hubs telemetry is implemented and live-tested locally; production point mapping and file upload remain open
**Priority:** P1 (production point mapping before rollout; file upload required before telemetry values can exceed 1 KiB)

## Goal

Add Building Communication (DTDPF) as a selectable telemetry destination while
preserving the existing connector and Building OS contracts. The first delivery
slice sends scalar telemetry to Azure Event Hubs. The next slices replace test
point metadata with production DTDPF mappings and implement contract ④: durable
upload of telemetry payloads larger than 1 KiB to GW Upload Storage, followed by
an Event Hubs notification that references the uploaded object.

```text
Connector → NATS JetStream → Normalizer → SQLite Outbox
                                              │
                         TELEMETRY_SINK=bos ──┴─→ Building OS gRPC
                         TELEMETRY_SINK=dtdpf ───→ Event Hubs
                                                     │
                         values > 1 KiB ─→ GW Upload Storage ─→ reference properties
```

DTDPF and Building OS telemetry are mutually exclusive per deployment in this
epic. Concurrent fan-out requires independent durable cursors and is out of
scope. Building OS remains the control-path authority; this epic does not add a
device registry or metadata authority to the gateway.

## Normative References

- `docs/DTDPF_GW設計要件書_v2_基盤非依存版.md`
  - Contract ① (`C1-01`–`C1-08`): Event Hubs body, properties, partitioning,
    and at-least-once delivery.
  - Contract ④ (`C4-01`–`C4-07`): 1 KiB threshold, file name/hash, 10 MiB
    maximum, and upload bindings.
  - `ING-16`/`ING-17`: durable acceptance and stable IDs on replay.
- `docs/adr/0008-selectable-dtdpf-event-hubs-sink.md` — selected-sink and
  DTDPF outbox decision.

## Acceptance Criteria

### Event Hubs telemetry

- [x] A deployment selects exactly one telemetry sink with
  `TELEMETRY_SINK=bos|dtdpf`; the existing Building OS protobuf is unchanged.
- [x] DTDPF events contain lowercase UUID v4 `id`, optional `type`, integer
  `rootId`, `dtId`, `topic`, UTC `eventTime`, and string `values.value`; obsolete
  `pointId` and `sequenceNumber` fields are not emitted.
- [x] EventData uses `application/json`, `CorrelationId=id`, string properties
  `rootId`/`dtId`, and `PartitionKey=string(rootId)`.
- [x] Events are batched per `rootId`; source order is retained within a
  partition key and oversized Event Hubs batches are split.
- [x] The DTDPF outbox uses SQLite WAL with `synchronous=FULL`, blocks rather
  than dropping old records when full, and acknowledges JetStream only after
  the SQLite commit.
- [x] A failed or partially completed Event Hubs checkpoint leaves the cursor
  unchanged and replays the same persisted UUID/body; cloud-side duplicate
  suppression by `id` remains required.
- [x] SAS connection strings both with and without `EntityPath` are accepted;
  when present, `EntityPath` must match `DTDPF_EVENTHUB_NAME`.
- [x] AMQP/TCP (5671) is the default and AMQP over WebSockets (443) is
  selectable with `DTDPF_EVENTHUB_TRANSPORT=websocket`.
- [ ] Production `rootId`/`dtId`/`topic`/`type` mappings are supplied and a real
  field connector event is observed by the DTDPF consumer.

### File upload (contract ④)

- [ ] The internal Common Event and durable telemetry record can carry an
  arbitrary `values` JSON object without breaking the current scalar numeric
  `value` path or Building OS protobuf projection.
- [ ] The attachment threshold is computed from compact UTF-8 JSON encoding of
  the `values` object and triggers only when its size is greater than 1,024
  bytes.
- [ ] Binding B is implemented for platform-independent deployments: the
  gateway uploads directly to GW Upload Storage using an agreed SAS URI or
  Entra ID credential. Binding A (IoT Edge local Blob auto-sync) is not required
  unless a deployment explicitly selects IoT Edge.
- [ ] Attachment bytes are stored durably before source acknowledgment and are
  retried across network loss and process restart. A telemetry event is not
  committed until both object upload and Event Hubs notification succeed.
- [ ] The object name is `{telemetry-id}.{extension}` and `fileHash` is the
  lowercase 64-character SHA-256 hex digest of the exact uploaded bytes.
- [ ] The Event Hubs message keeps the normal body with a summary
  `values.value` and adds string properties `fileUpload=1`, `fileName`, and
  `fileHash` only after upload succeeds.
- [ ] Payloads above 10 MiB are not uploaded or chunked; a summary event is sent
  without file properties and the condition is logged and metered.
- [ ] Credentials and SAS URIs are injected through environment/secrets only,
  are never logged, and are excluded from fixtures, test output, and Git.
- [ ] Unit tests cover byte-threshold boundaries (1,024/1,025), SHA-256 and file
  naming, 10 MiB handling, upload retry, crash replay, and upload-success /
  Event-Hubs-failure duplicate behavior.
- [ ] A live non-production test uploads one object, verifies its hash and size,
  sends the matching Event Hubs reference, and confirms cloud-side retrieval.

## Child Features

- [x] **FEAT-046: Selectable DTDPF Event Hubs sink.** Internal telemetry
  record, stable UUID, DTDPF encoder, Azure SDK v2 producer, `rootId`
  batching, AMQP/WebSocket transport, reconnecting uplink, secure env
  configuration, Compose override, and contract tests. Landed via PR #168.
- [ ] **FEAT-047: Production DTDPF point mapping.** Replace the simulation
  fixture with approved mappings. Existing Point List
  `(connector_id, local_id) → point_id` remains authoritative for native
  resolution; `point_id` must equal DTDPF `dtId`. Validate exact protocol names
  and run a real connector-to-cloud test.
- [ ] **FEAT-048/049/050: File upload (contract ④).** Scoped in
  [PRD-001](../prd/PRD-001-dtdpf-file-upload.md): structured telemetry
  values, a GW Upload Storage client, and durable attachment orchestration so
  a `values` payload > 1 KiB is uploaded and referenced instead of inlined.
- [ ] **FEAT-051: DTDPF configuration lifecycle.** Replace static local
  `pointConfig.json` with generation-based atomic reload and, when its external
  contract is approved, GW integration API synchronization. Rules and telemetry
  type schema loading remain separate follow-ups.

## External Information Required

### Production telemetry mapping (FEAT-047)

For every point, obtain from the DTDPF/platform and field teams:

| Source | Required fields |
|---|---|
| DTDPF | `rootId`, `dtId`, `topic`, `type`, `typeId`, `protocol` |
| Gateway/field | `connector_id`, `local_id`, `unit`, `device_ref` |

`point_id` in the gateway Point List must exactly equal DTDPF `dtId`.
`protocol` matching is case-sensitive in the current resolver.

### Upload contract (FEAT-049/050)

The DTDPF/platform owner must confirm before implementation:

1. Storage account endpoint and container name for GW Upload Storage.
2. Binding B authentication: per-object/container SAS or Entra ID.
3. How SAS is provisioned and refreshed: pre-distributed secret or GW
   integration API extension.
4. Required object path beyond the normative `{id}.{extension}` file name.
5. MIME type and supported extensions for each telemetry type.
6. Whether Event Hubs notification must wait for storage service acknowledgment
   (recommended and assumed by this backlog).
7. Retention, lifecycle deletion, and cloud-side duplicate handling for an
   object uploaded before a retried Event Hubs notification.
8. Representative payloads and schemas that exceed 1 KiB.

## Handoff (2026-09-09)

### Repository state

- Branch: `master`; base commit: `cb05fbe`.
- Target remote: `tk` → `https://github.com/TAKENAKA-Corp/nexus-gateway-tk.git`.
- FEAT-046 changes are present in the working tree but are **not committed or
  pushed**.
- `.env` contains local credentials and is ignored by Git. Never add it to a
  commit or paste its values into logs/issues. `.env.example` is safe to track.
- `connector/opcua/bin/` is an unrelated untracked build output and must not be
  included in the DTDPF commit.

### Implemented files

- `internal/telemetry/record.go`: sink-independent durable record.
- `internal/dtdpf/config.go`: strict `pointConfig.json` loader/resolver.
- `internal/dtdpf/encoder.go`: DTDPF body/properties encoder.
- `internal/dtdpf/sink.go`: Event Hubs batching and AMQP/WebSocket producer.
- `internal/dtdpf/uplink.go`: reconnect/replay runner.
- `internal/storeforward/buffer.go`, `pump.go`: durable metadata, legacy row
  preparation, blocking overflow, and Ack-after-commit.
- `internal/normalizer/normalizer.go`: stable UUID generation and DTDPF
  metadata enrichment.
- `cmd/gateway/main.go`: sink and transport selection.
- `docker-compose.dtdpf.yml`, `.env.example`,
  `fixtures/pointConfig.dtdpf.json`: local runtime configuration.
- `internal/dtdpf/live_test.go`, `live_pipeline_test.go`: opt-in Azure tests
  guarded by build tag `integration` and `DTDPF_LIVE_TEST=true`.

### Verified behavior

- Focused unit/integration tests pass for DTDPF, Normalizer, Store-and-Forward,
  Uplink, and Gateway configuration.
- `go build ./...` passes.
- `docker compose -f docker-compose.yml -f docker-compose.dtdpf.yml config`
  succeeds (base Compose emits an existing obsolete-`version` warning).
- Direct Event Hubs sink probe succeeded against `testevthub`.
- Full live path succeeded:
  `JetStream → Normalizer → SQLite Outbox → Forwarder → Event Hubs`.
  Observed state was `written=1`, `sent=1`, `depth=0`, `cursor=1`.
- Native AMQP/TLS on 5671 was reset by the current network. AMQP over
  WebSockets on 443 succeeded; local `.env` therefore selects `websocket`.
- Azure management metrics were not queried during the run because the Azure
  CLI MSAL cache required `az login`; Event Hubs SDK send acknowledgment was
  successful.

### Resume commands

Normal regression and build (does not send live data):

```powershell
go test ./internal/dtdpf ./internal/normalizer ./internal/storeforward ./internal/uplink ./cmd/gateway
go build ./...
```

Live tests are intentionally opt-in. Load the ignored `.env` into the process,
set `DTDPF_LIVE_TEST=true`, and run only the named integration test. Do not echo
the connection string. The most representative test is:

```powershell
go test -tags=integration ./internal/dtdpf -run '^TestLiveDTDPFTelemetryPipeline$' -count=1 -v
```

## Dependencies and Sequencing

1. Complete FEAT-047 before calling Event Hubs telemetry production-ready.
2. Agree the upload contract questions above before FEAT-049 implementation.
3. Implement FEAT-048 before FEAT-050 because the current Common Event carries
   only one `float64` value and cannot represent an attachment-sized `values`
   object.
4. Implement FEAT-049 before FEAT-050; orchestration depends on a tested storage
   client.
5. Implement FEAT-051 only after the cloud GW integration API contract is
   confirmed. Static configuration is sufficient for FEAT-047 validation.

## Non-Goals for This Epic

- Concurrent Building OS and DTDPF telemetry fan-out.
- IoT Hub Direct Method control (`setProperty`, `invokeAction`,
  `configUpdateNotify`).
- Cloud/Gateway rule evaluation and `action`/`actionIds`/`ruleIds` properties.
- IoT Edge Binding A unless a deployment explicitly requires it.
- Device registry, metadata registry, or command-authority responsibilities.
