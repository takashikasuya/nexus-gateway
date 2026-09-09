# Selectable DTDPF Event Hubs telemetry sink

- **Date:** 2026-09-09
- **Status:** Accepted

## Context

The gateway originally delivered normalized telemetry only to Building OS over
gRPC. Building Communication (DTDPF) requires a different outbound contract:
Azure Event Hubs batches partitioned by `rootId`, a DTDPF JSON body, and stable
UUIDs for cloud-side duplicate suppression. The DTDPF contract requires
at-least-once delivery after durable acceptance, while ADR-0002 deliberately
allows drop-oldest behavior for the Building OS best-effort path.

## Decision

Deployments select exactly one telemetry sink with `TELEMETRY_SINK=bos|dtdpf`.
The public Building OS protobuf remains unchanged. A sink-independent internal
record carries the stable event ID and optional DTDPF metadata through the
SQLite outbox.

DTDPF metadata is loaded from a local `pointConfig.json` and joined after the
existing Point List resolves `local_id` to `point_id`; `point_id` equals DTDPF
`dtId`. DTDPF uses the Azure Event Hubs SDK v2, SAS connection-string
authentication, and `rootId` as the partition key. AMQP/TCP is the default
binding; AMQP over WebSockets is configurable for networks that require port
443.

The DTDPF outbox uses SQLite WAL with `synchronous=FULL`, does not evict old
records when full, and acknowledges JetStream only after the SQLite commit.
Backpressure keeps the source message alive with progress acknowledgements.
Event Hubs send failures leave the cursor unchanged, so replay uses the same
persisted ID and body.

## Consequences

- Building OS and DTDPF are not sent concurrently; independent multi-sink
  cursors are deferred.
- Existing Building OS deployments retain ADR-0002 drop-oldest behavior.
- DTDPF startup fails if required configuration or metadata for legacy pending
  rows cannot be resolved.
- The initial DTDPF scope excludes rule properties, file attachments, IoT Hub
  Direct Methods, GW API configuration sync, and Entra ID. WebSockets was added
  after live validation showed native AMQP/TLS was blocked on the test network.
- Cloud consumers must deduplicate by the telemetry `id` field.