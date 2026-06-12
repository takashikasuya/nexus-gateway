# EP-003: Normalizer, gRPC Uplink & Local Buffer

**Status:** Draft
**Priority:** P0

## Goal

This epic delivers the path from raw connector events to Building OS: the Normalizer unifies identifiers/units/quality/timestamps, the gRPC uplink streams telemetry under the stable Building OS contract, and the SQLite buffer guarantees no data loss during outages. This is the gateway's core value proposition.

## Acceptance Criteria

- [ ] Normalizer strips protocol-dependent info and unifies Point ID, Device ID, timestamp, quality, and unit.
- [ ] Semantic mapping (REC/Brick/QUDT/BOT) is hosted in the Normalizer (interface stubbed for MVP), never in connectors.
- [ ] Protobuf API defines TelemetryService (streaming Publish → Ack), CommandService (Execute → CommandResult), and DiscoveryService (Discover → stream DeviceInfo), managed with Buf incl. breaking-change detection.
- [ ] Telemetry is streamed to Building OS over gRPC and only over gRPC.
- [ ] SQLite store-and-forward buffers telemetry during uplink outage and forwards in order on reconnect with no loss.

## Child Features

- [ ] FEAT-011: Protobuf/Buf API definitions (Telemetry, Command, Discovery)
- [ ] FEAT-012: Normalizer (ID/unit/quality/timestamp unification)
- [ ] FEAT-013: gRPC Telemetry uplink (streaming)
- [ ] FEAT-014: SQLite store-and-forward buffer
- [ ] FEAT-015: Semantic mapping interface (stub)
