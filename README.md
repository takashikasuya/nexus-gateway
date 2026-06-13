# nexus-gateway

**An edge integration gateway that connects building equipment (BMS, IoT, field protocols) to [Building OS](https://github.com/takashikasuya/gutp-building-os-oss).**

It collects equipment data, relays control, absorbs protocol differences, and
normalizes everything into Building OS's common data model. Building OS is the
**System of Record**; this gateway is responsible only for **connection and
translation**.

---

## Why this exists

A building has dozens of equipment protocols — BACnet, OPC-UA, MQTT, Modbus —
each with its own addressing and semantics. Building OS wants a single,
canonical telemetry/control contract keyed by `(gateway_id, point_id)`. Something
has to sit at the edge and absorb the protocol diversity.

### Why not EdgeX Foundry?

EdgeX Foundry is an excellent **general-purpose IoT edge platform** — it targets
buildings, factories, energy, retail, and healthcare alike, and ships Device
Services, Core Metadata, Core Command, an Application Service, a Message Bus, and
a Security stack. A minimal deployment is easily **10–20 containers**.

For this project that generality is a cost, not a benefit, because EdgeX's
**Core Metadata** (Device/Profile registry, Provision Watcher) and **Core
Command** (REST → Device Service) duplicate responsibilities that **Building OS
already owns**: the Digital Twin (REC/Brick/Ditto) is the device & point
registry, and the command path is Building OS → gRPC → gateway. Adopting EdgeX
wholesale would mean operating a second registry and command service alongside
the ones Building OS already provides — which is the main reason it reads as
"heavy" here.

So nexus-gateway is deliberately closer to **Azure IoT Edge + a protocol adapter
+ a gRPC uplink** than to a full IoT platform. It still **borrows EdgeX's good
ideas** — the *Device Service* structure, *connector separation*, and the
*Common Event → pipeline* flow — without the platform weight. The connector
contract is essentially:

```
discover() → Stream[Device]
subscribe() → Stream[Telemetry]
write(cmd)  → Result
```

with the proven per-protocol OSS stacks underneath: **Eclipse Milo** (OPC-UA),
**BACpypes3** (BACnet), **Eclipse Paho** (MQTT).

---

## Architecture

```
   field equipment / simulators
        │  BACnet/IP · OPC-UA · MQTT
        ▼
  ┌─────────────┐   evt.<proto>.<id>   ┌────────────┐   TelemetryFrame   ┌──────────────────┐
  │ Connectors  │ ───────────────────▶ │ Normalizer │ ─────────────────▶ │ Store-and-Forward │
  │ (1 / proto) │   NATS JetStream     │ local_id→  │   (point_id)       │ (SQLite ring buf) │
  └─────────────┘   stream EVENTS      │  point_id  │                    └────────┬─────────┘
        ▲                              └────────────┘                              │ gRPC stream
        │ cmd.<proto>.<id>  (core NATS request-reply)                              ▼
  ┌─────────────┐        ┌──────────┐   ControlCommand   ┌────────────┐   GatewayIngress/StreamTelemetry
  │ Egress      │ ◀───── │ Dispatch │ ◀───────────────── │ Building OS │ ◀──────────────────────────────
  │ agent       │  gRPC GatewayEgress/Connect            └────────────┘   (mTLS at the Envoy edge)
  └─────────────┘
```

- **Connectors** (one isolated container per protocol) talk to equipment and
  publish **Common Events** carrying *native addressing only* — no canonical
  identity ([ADR-0001](docs/adr/0001-telemetry-pipeline-shape.md)).
- The **Normalizer** is the single durable consumer on `evt.>`. It joins the
  **Point List** to resolve `local_id → point_id` and emits **TelemetryFrames**
  (`gateway_id` + `point_id` + value + ts).
- **Store-and-Forward** is a bounded SQLite ring buffer: best-effort,
  drop-oldest, at-least-once up to Building OS
  ([ADR-0002](docs/adr/0002-best-effort-store-and-forward.md)).
- The **Ingress Uplink** streams frames to Building OS's `GatewayIngress`
  service; the **Egress agent** holds the `GatewayEgress` stream and dispatches
  inbound **Control Commands** to connectors over a deadline-bounded, idempotent
  core-NATS request-reply ([ADR-0004](docs/adr/0004-control-path-reliable-within-window.md)).

### Key decisions (ADRs)

| ADR | Decision |
|-----|----------|
| [0001](docs/adr/0001-telemetry-pipeline-shape.md) | Connectors emit native addressing; the Normalizer owns `local_id → point_id`. Wire identity is `(gateway_id, point_id)` only. |
| [0002](docs/adr/0002-best-effort-store-and-forward.md) | Store-and-Forward is best-effort (bounded ring buffer, drop-oldest, at-least-once). |
| [0003](docs/adr/0003-point-list-source-of-truth.md) | The Point List's source of truth is the Building OS twin; the gateway syncs a copy by diff. Provisioning sync > file/CSV bootstrap. |
| [0004](docs/adr/0004-control-path-reliable-within-window.md) | Control is real-time-or-fail: deadline-bounded core-NATS request-reply, idempotent on `control_id`. |
| [0005](docs/adr/0005-jetstream-topology-bounded-replay.md) | JetStream sits before the Normalizer as the durable replay/back-pressure boundary. |
| [0006](docs/adr/0006-connector-distribution-signed-oci.md) | Connectors are signed OCI images, run digest-pinned, installed via the Connector Catalog with cosign verification + rollback. |
| [0007](docs/adr/0007-transport-security-mtls-at-edge.md) | Gateway↔Building OS gRPC is mTLS terminated at the Building OS Envoy edge (`gateway_id` ↔ client-cert CN/SAN); h2c in-cluster. |

---

## Features

- **Protocol connectors** — BACnet (Python/BACpypes3), OPC-UA (Java/Eclipse
  Milo), MQTT (Go/Paho), plus a zero-dependency `sim` connector for smoke tests.
  Each is an isolated container holding no Building-OS domain model.
- **Telemetry + control** in one gateway: uplink streaming and a write path
  (BACnet WriteProperty, OPC-UA Write/Method, MQTT publish).
- **Point List sync** from Building OS (or a file/CSV stand-in) with diff
  convergence; near-static, so synced once then slow-polled.
- **Resilience**: bounded Store-and-Forward rides out Building OS outages; the
  Normalizer drops-and-meters poison / point-list-miss events
  (`normalizer_invalid_total`, `normalizer_unresolved_total`).
- **Security**: config-driven **TLS/mTLS** to Building OS; **Keycloak/OIDC**
  protects the Admin API & UI (operator/viewer roles).
- **Admin UI** (Next.js) — dashboard + connector lifecycle (start/stop/restart/
  upgrade), behind OIDC.
- **Lifecycle management** via the Docker Engine API; **signed-OCI** connector
  distribution through the Connector Catalog (digest-pinned, cosign-verified,
  stop→replace→health→rollback).

---

## Quickstart

```bash
# Full stack: NATS + mock Building OS + gateway + Keycloak + Admin UI
docker compose up --build
```

- Admin UI: http://localhost:3000 (Keycloak realm `nexus-gateway`; users
  `operator`/`operator`, `viewer`/`viewer`)
- Gateway Admin API: http://localhost:8080 (`/health`, `/metrics`, `/connectors`)
- Keycloak: http://localhost:8090

Run the gateway binary directly:

```bash
go run ./cmd/gateway --dev-sim   # in-process sim connector for a no-equipment smoke run
```

### Configuration (flags / env)

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--nats` | `NATS_URL` | `nats://localhost:4222` | NATS URL |
| `--bos` | `BOS_ADDR` | `localhost:50051` | Building OS gRPC address |
| `--gateway-id` | `GATEWAY_ID` | `gw-001` | Gateway identity (also the mTLS cert CN/SAN) |
| `--bos-insecure` | `BOS_INSECURE` | `false` | Plaintext h2c to Building OS — dev/CI only (ADR-0007) |
| `--bos-ca` / `--bos-cert` / `--bos-key` | `BOS_CA_FILE` / … | – | TLS/mTLS material |
| `--provisioning-url` | `PROVISIONING_URL` | – | Building OS Point List provisioning API |
| `--provisioning-file` | `PROVISIONING_FILE` | – | File/CSV-backed Point List (dev/E2E) |
| `--point-sync-interval` | – | `10m` | Point List poll interval after initial sync |
| `--admin-jwks-url` | `KEYCLOAK_JWKS_URL` | – | Keycloak JWKS (empty = Admin API auth disabled) |
| `--dev-sim` | `DEV_SIM` | `false` | Run the in-process sim connector (non-production) |

### Simulator integration (no equipment)

The sibling repos `../bacnet-sim-gateway` and `../opcua-sim-gateway` provide
standard-compliant BACnet/IP and OPC-UA simulators. See
[`fixtures/integration/`](fixtures/integration/README.md):

```bash
docker compose -f docker-compose.yml -f docker-compose.integration.yml --profile opcua up
```

---

## Extending: add a protocol connector

A connector is an independent container that:

1. reads only the **native addresses** it must poll/subscribe from the Point List;
2. publishes **Common Events** to JetStream subject `evt.<protocol>.<connector_id>`
   carrying `protocol` + native `local_id` + value/unit/quality/timestamp — **no
   `point_id`** (the Normalizer assigns it);
3. subscribes to `cmd.<protocol>.<connector_id>` for **Control Commands** and
   replies with a typed result, idempotent on `control_id`.

Use the per-language reference connectors (`connector/{bacnet,opcua,mqtt}`) as
templates. Package it as a signed OCI image and list it in the Connector Catalog
to have the Core Agent run it digest-pinned (ADR-0006).

---

## Development

```bash
go build ./...
go test -race ./...           # Go pipeline + connectors
cd admin-ui && npm run type-check && npm run build
```

CI (`.github/workflows/ci.yml`) runs the Go build/test and the Admin UI
type-check/build on every PR.

---

## License

Apache-2.0, consistent with the SBCO / Building OS sibling projects. See
[`LICENSE`](LICENSE).
