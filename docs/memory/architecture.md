---
name: architecture
description: System architecture overview for nexus-gateway
metadata:
  type: project
---

# Architecture

> Status: initial skeleton — fill in as design firms up.

## System Diagram

```
                 Building OS
                       ▲
                       │  gRPC (only contract)
                       │
┌────────────────────────────────────┐
│ Integration Gateway                │
│                                    │
│  Admin UI                          │
│      │                             │
│      ▼                             │
│  Core Agent (Go)                   │
│      ├── Connector Registry        │
│      ├── Lifecycle Manager         │
│      ├── Config Manager            │
│      ├── Health Monitor            │
│      └── Admin API                 │
│                                    │
│  Normalizer                        │
│      ▼                             │
│  NATS JetStream                    │
│                                    │
│  Connector Containers              │
│      ├── BACnet (Python)           │
│      ├── OPC-UA (Java)             │
│      ├── MQTT (Go/Python)          │
│      ├── Modbus (Go/Python)        │
│      └── Future Extensions         │
│                                    │
│  SQLite Buffer (store & forward)   │
│  OpenTelemetry Collector           │
└────────────────────────────────────┘
```

## Components

| Component | Responsibility |
|-----------|---------------|
| Core Agent (Go) | Orchestrates connector lifecycle, config, health, and exposes the Admin API. Manages containers via Docker Engine SDK. |
| Connector Registry | Tracks available/installed connectors and their versions. |
| Lifecycle Manager | Start/Stop/Restart/Upgrade connector containers. |
| Config Manager | Distributes and persists connector + gateway configuration. |
| Health Monitor | Tracks gateway and connector health (uptime, CPU, memory, disk). |
| Admin API | Backend for the Admin UI; OIDC/OAuth2 protected. |
| Connectors | Per-protocol independent containers. Emit common event format only; no equipment-specific models; no inter-connector dependencies. |
| Normalizer | Strips protocol-dependent info; unifies Point ID, Device ID, timestamp, quality, unit. Hosts future semantic mapping (REC/Brick/QUDT/BOT). |
| NATS JetStream | Internal pub/sub transport for connector events; replay and short-term retention. |
| SQLite Buffer | Local store-and-forward during uplink outages. |
| gRPC Uplink | Stable contract to Building OS: TelemetryService, CommandService, DiscoveryService. |
| OpenTelemetry Collector | Collects metrics/logs/traces; exports OTLP + Prometheus. |
| Admin UI | React/Next.js operator console. |

## Technology Stack

- **Core Agent:** Go — grpc-go, Docker Engine SDK, nats.go, OpenTelemetry SDK.
- **API definition:** Protocol Buffers, managed with Buf (schema mgmt, codegen, breaking-change detection).
- **Messaging:** NATS JetStream.
- **Local buffer:** SQLite.
- **Connectors:** BACnet (Python — BACpypes3, BAC0), OPC-UA (Java — Eclipse Milo), MQTT (Go/Python, MQTT 5.0), Modbus (Go/Python, Modbus TCP).
- **Admin UI:** React, Next.js, shadcn/ui, TanStack Table.
- **Auth:** Keycloak (OIDC/OAuth2) for Admin UI, Admin API, Building OS connection.
- **Observability:** OpenTelemetry → OTLP, Prometheus.

## Common Event Model (connector output)

```json
{
  "gateway_id": "gw-001",
  "protocol": "opcua",
  "device_id": "ahu-01",
  "point_id": "supply_air_temp",
  "value": 23.4,
  "unit": "Cel",
  "quality": "Good",
  "timestamp": "2026-01-01T00:00:00Z"
}
```

## Open Questions

- [ ] Connector ↔ Core transport: NATS subjects/stream layout and back-pressure strategy.
- [ ] SQLite buffer retention policy, ordering guarantees, and replay cursor semantics under uplink recovery.
- [ ] Normalizer Point/Device ID mapping configuration format and source of truth.
- [ ] Connector upgrade flow (image pull, version pinning) given OTA is out of scope for MVP-1.
- [ ] gRPC stream Telemetry batching/acking granularity vs. NATS JetStream consumer model.
