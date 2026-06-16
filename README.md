# nexus-gateway

**An edge integration gateway that connects building equipment (BMS, IoT, field protocols) to [Building OS](https://github.com/takashikasuya/gutp-building-os-oss).**

*English / [日本語](README.ja.md)*

It collects equipment data, relays control, absorbs protocol differences, and
normalizes everything into Building OS's common data model. Building OS is the
**System of Record**; this gateway is responsible only for **connection and
translation**.

> **Status: pre-MVP.** The MVP scope (in/out), pass conditions, and remaining
> gaps are fixed in **[MVP_READINESS.md](MVP_READINESS.md)** — OPC-UA is the
> baseline end-to-end protocol; BACnet, Envoy mTLS, and cosign production are
> MVP+1.

---

## Why this exists

> 📄 For the full positioning, system comparison (EdgeX / Azure IoT Edge / Kura /
> Hono / ThingsBoard / EMQX Neuron / OpenRemote), responsibility split with
> Building OS / SBCO, and the technical-challenge analysis, see
> **[docs/background.md](docs/background.md)** ([日本語](docs/background.ja.md)).

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

> 🚀 New here? The **[Getting Started guide](docs/getting-started.md)**
> ([日本語](docs/getting-started.ja.md)) walks you from `compose up` to watching
> telemetry flow and driving a connector through the Admin API, in ~10 minutes
> with no equipment.

```bash
# 1. Full stack: NATS + mock Building OS + gateway + Keycloak + Admin UI
docker compose up --build

# 2. Verify healthy (all services should reach "healthy" within ~60 s)
docker compose ps
```

| Endpoint | URL | Notes |
|----------|-----|-------|
| Admin UI | http://localhost:13000 | Keycloak realm `nexus-gateway`; users `operator`/`operator`, `viewer`/`viewer` |
| Gateway Admin API | http://localhost:18080 | `/health`, `/metrics`, `/connectors` |
| Keycloak | http://localhost:18090 | Admin: `admin`/`admin` |
| mock Building OS (gRPC) | `localhost:15051` | `GatewayIngressService` stub for dev |
| NATS | `localhost:14222` | NATS client port; monitoring at `:18222` |

Run the gateway binary directly (no Docker):

```bash
go run ./cmd/gateway --dev-sim   # in-process sim connector for a no-equipment smoke run
```

### Configuration (flags / env)

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--nats` | `NATS_URL` | `nats://localhost:4222` | NATS URL |
| `--bos` | `BOS_ADDR` | `localhost:50051` | Building OS gRPC address |
| `--gateway-id` | `GATEWAY_ID` | `gw-001` | Gateway identity (also the mTLS cert CN/SAN) |
| `--admin-addr` | `ADMIN_ADDR` | `:8080` | Admin API listen address |
| `--admin-jwks-url` | `KEYCLOAK_JWKS_URL` | – | Keycloak JWKS URL (empty = auth disabled) |
| `--admin-audience` | `KEYCLOAK_AUDIENCE` | `account` | Expected JWT audience |
| `--admin-issuer` | `KEYCLOAK_ISSUER` | – | Expected JWT issuer |
| `--point-list` | `POINT_LIST_FILE` | `fixtures/point_list.json` | Bootstrap fixture (used when no provisioning source) |
| `--point-list-persist` | `POINT_LIST_PERSIST` | `data/point_list.json` | Path to persist the synced Point List across restarts |
| `--provisioning-url` | `PROVISIONING_URL` | – | Building OS Point List provisioning API |
| `--provisioning-file` | `PROVISIONING_FILE` | – | File/CSV-backed Point List (dev/E2E) |
| `--provisioning-connector-id` | `PROVISIONING_CONNECTOR_ID` | `bacnet-01` | Connector ID stamped on CSV-loaded entries |
| `--point-sync-interval` | – | `10m` | Point List poll interval after initial sync |
| `--sf-db` | `SF_DB` | `data/storeforward.db` | Store-and-Forward SQLite database path |
| `--sf-cap` | – | `100000` | Store-and-Forward ring buffer capacity (frames) |
| `--bos-insecure` | `BOS_INSECURE` | `false` | Plaintext h2c to Building OS — dev/CI only (ADR-0007) |
| `--bos-ca` | `BOS_CA_FILE` | – | PEM CA bundle to verify the Building OS server cert |
| `--bos-cert` | `BOS_CERT_FILE` | – | Client certificate for mTLS to Building OS |
| `--bos-key` | `BOS_KEY_FILE` | – | Client private key for mTLS to Building OS |
| `--bos-servername` | `BOS_SERVER_NAME` | – | Override server name in Building OS cert verification |
| `--dev-sim` | `DEV_SIM` | `false` | Run the in-process sim connector (non-production, ADR-0001) |
| `--catalog-file` | `CATALOG_FILE` | – | File-backed Connector Catalog (JSON `[]Manifest`); enables `POST /connectors/{name}/install` |
| `--catalog-url` | `CATALOG_URL` | – | Remote Connector Catalog base URL (overrides `--catalog-file`) |
| `--catalog-allowlist` | `CATALOG_ALLOWLIST` | `ghcr.io` | Comma-separated list of allowed OCI registries (ADR-0006) |

### Simulator integration (no equipment)

The sibling repos `../bacnet-sim-gateway` and `../opcua-sim-gateway` provide
standard-compliant BACnet/IP and OPC-UA simulators. See
[`fixtures/integration/`](fixtures/integration/README.md):

```bash
# OPC-UA E2E (CI-friendly, plain TCP)
docker compose -f docker-compose.yml -f docker-compose.integration.yml --profile opcua up

# BACnet E2E (requires host networking for Who-Is/I-Am broadcast)
docker compose -f docker-compose.yml -f docker-compose.integration.yml --profile bacnet up
```

### Running with a live Building OS

Point the gateway at the real Building OS stack instead of mock-bos:

```bash
# Building OS OSS stack (see github.com/takashikasuya/gutp-building-os-oss)
docker compose -f /path/to/gutp-building-os-oss/docker-compose.oss.yaml up -d

# Start gateway with BOS ingress + egress addresses and the SoS Point List
GATEWAY_ID=GW-SOS-001 \
BOS_ADDR=localhost:5051 \
BOS_INSECURE=true \
PROVISIONING_FILE=/path/to/mvp-pointlist.csv \
go run ./cmd/gateway
```

For the full E2E test suite against Building OS, see
**[`docs/e2e-test-overview.md`](docs/e2e-test-overview.md)**.

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

### Module seams (testability)

The behaviors the ADRs describe are concentrated into **deep modules** — a small
interface that is also the unit-test surface, so each is exercised in-process
without a live NATS/gRPC/Docker stack ([EP-011](docs/backlog/epic/EP-011-architecture-deepening.md)):

| Module | Seam | What it owns |
|--------|------|--------------|
| `uplink.Forwarder` | `FrameSink` (`Send` + `Checkpoint`) | ADR-0002 checkpoint/advance/drift/replay policy; gRPC client-streaming is the `grpcSink` adapter. |
| `lifecycle.HealthMonitor` | `GatewayMetrics` + `ConnectorProber` | host stats (uptime/mem/disk) vs. container liveness, each probed and tested independently. |
| `pointlist.Resolver` / `ReverseResolver` | forward + reverse lookup | the single Point List, consumed by the Normalizer (forward) and control Dispatch (reverse). |
| `adminapi` | `NewServer` / `NewSecureServer` + `ServerOptions` | one no-auth and one JWT constructor over a shared builder; optional sources via a single struct. |

E2E tests in `integration/` require a live stack and skip automatically
without the relevant `E2E_*` env vars (ADR-0004). See
**[`docs/e2e-test-overview.md`](docs/e2e-test-overview.md)** for the full
test landscape — in-process CI tests, live connector stack tests, and
Building OS integration tests (#43–#46).

---

## Contributing & security

- **Contributing** — dev setup, test gates, and PR conventions are in
  [`CONTRIBUTING.md`](CONTRIBUTING.md). Start from the
  [Getting Started guide](docs/getting-started.md).
- **Security** — report vulnerabilities privately via
  [`SECURITY.md`](SECURITY.md) (GitHub private advisories); please don't open a
  public issue for them.

---

## License

Apache-2.0, consistent with the SBCO / Building OS sibling projects. See
[`LICENSE`](LICENSE).
