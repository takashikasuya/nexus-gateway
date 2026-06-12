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

## Pending Decisions

- NATS subject/stream topology and back-pressure handling → see ADR when resolved.
- SQLite buffer ordering/retention/replay semantics → see ADR when resolved.
- Normalizer ID-mapping configuration format and source of truth → see ADR when resolved.
- MQTT and Modbus connector language choice (Go vs Python) → to be settled per connector.
