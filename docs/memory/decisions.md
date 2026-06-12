---
name: decisions
description: Key design decisions and their rationale for nexus-gateway
metadata:
  type: project
---

# Design Decisions

> Cross-reference with docs/adr/ for formally recorded decisions.
> This file captures informal or in-progress reasoning.

## Settled Decisions (from design requirements)

- **Building OS is the System of Record.** Gateway responsibility is limited to connection + translation. No device registry, metadata registry, or command authority here. → candidate ADR-001.
- **Azure IoT Edge-like structure, not EdgeX Foundry / Eclipse Hono / Kubernetes.** Chosen to avoid duplicating Building OS responsibilities and to stay lightweight for building-edge use. → candidate ADR-002.
- **gRPC is the only Building OS contract.** Internal implementation may change; the gRPC API must not break. Managed via Protobuf + Buf with breaking-change detection. → candidate ADR-003.
- **Per-protocol connector containers, fully isolated.** No inter-connector dependencies; connectors emit common event format only. → candidate ADR-004.
- **Semantic mapping (REC/Brick/QUDT/BOT) belongs in the Normalizer**, never in connectors. → candidate ADR-005.
- **Go for the Core Agent** — Docker SDK affinity, concurrency, single binary, cross-platform.
- **NATS JetStream** for internal connector event transport (pub/sub, replay, short-term retention).
- **SQLite** for local store-and-forward buffering during uplink outages.
- **Keycloak (OIDC/OAuth2)** for Admin UI, Admin API, and Building OS connection auth.

## Settled in design review (grill-with-docs session, 2026-06-13)

- **Telemetry pipeline = Connectors → NATS JetStream → Normalizer → SQLite Store-and-Forward → gRPC Uplink.** JetStream sits *before* normalization so raw Common Events can be replayed and re-normalized after a Point List change. → candidate ADR.
- **Building OS contract is authoritative over design doc §8.** Real proto lives in `../gutp-building-os-oss/proto/` (package `gatewaybridge`). Ingress = `GatewayIngress/StreamTelemetry` (client-streaming `TelemetryFrame{gateway_id, point_id, double value, RFC3339 timestamp, attributes}`). Egress = `GatewayEgress/Connect` (gateway-dialed bidi; `ControlCommand{control_id, point_id, present_value, priority}` / `ControlResult`). No DiscoveryService.
- **Wire identity is `(gateway_id, point_id)` only.** `building_id`/`device_id` are not sent; Building OS derives them from the twin (#181).
- **Normalizer owns native→`point_id` resolution** via the Point List. Connectors emit native addressing only.
- **`value` is `double` only** (bool→0/1, state/enum→numeric code; non-numeric → `attributes`). No `oneof` (deferred on Building OS side, #189). `timestamp` is RFC3339, always set to source observation time.
- **Delivery is best-effort** (slight loss / reordering acceptable, per product owner). At-least-once with bounded duplicates on reconnect; no exactly-once. Store-and-Forward = bounded SQLite **ring buffer** (drop-oldest on overflow). Cursor advances per batch on `StreamAck.accepted`; `accepted < sent` is surfaced as a **per-`point_id` drift counter** (no dead-letter table). → candidate ADR.
- **Point List source of truth = Building OS twin** (OxiGraph `sbco:PointExt`). Gateway pulls + diffs to converge. Requires a new gateway-scoped, versioned point-list provisioning API on Building OS → `gutp-building-os-oss` issue #224.
- **Control path is reliable-within-window, never replayed** (ADR-0004). Core Agent holds the Egress bidi stream; Core→Connector via core NATS request-reply (no persistence); always returns one definitive (typed) `ControlResult`; Core crash mid-command → Building OS timeout → operator retries with new `control_id`. Same Point List resolver serves both directions (native↔point_id).

## Pending Decisions

- NATS subject/stream topology and back-pressure handling → see ADR when resolved.
- SQLite buffer ordering/retention/replay semantics → see ADR when resolved.
- Normalizer ID-mapping configuration format and source of truth → see ADR when resolved.
- MQTT and Modbus connector language choice (Go vs Python) → to be settled per connector.
