# Transport security: mTLS terminated at the Building OS edge, h2c in-cluster

**Status:** accepted — 2026-06-13

## Context

The gateway's two gRPC links to Building OS — the telemetry **Ingress Uplink** (`gatewaybridge.GatewayIngress/StreamTelemetry`) and the control **Egress Uplink** (`gatewaybridge.GatewayEgress/Connect`) — currently use `insecure.NewCredentials()`. That is PoC-grade: the link crosses a trust boundary (the gateway is outside the Building OS cluster) and carries the `(gateway_id, point_id)` identity that Building OS treats as authoritative for twin ownership. We need to fix how this machine-to-machine link is authenticated and encrypted before any non-lab deployment, and the choice must match what Building OS actually enforces — not what we wish it enforced.

Confirmed from the Building OS side (`gutp-building-os-oss`):

- The gRPC ingress lives in **ConnectorWorker** (`GatewayIngressService`); the gRPC egress lives in **GatewayBridge** (`GatewayEgressService`). Both Kestrel hosts listen **HTTP/2 plaintext (h2c)** in-cluster; the source comment states *"TLS/mTLS terminates at the ingress"* (Envoy/Contour/Emissary), tracked as Building OS #161.
- `GatewayIngressService` performs **authorization only**: it checks that the frame's `gateway_id` owns the `point_id` and rejects unknown/non-owned points. It does **not** validate any certificate or token — origin authentication is delegated to the mTLS edge.
- The intended production topology is: **GW ⇄ gRPC over mTLS ⇄ Envoy edge ⇄ h2c ⇄ ConnectorWorker/GatewayBridge**, with client certificates issued/rotated by **cert-manager** and the gateway's identity (`gateway_id`) bound to the client cert **CN/SAN**.
- There is **no OIDC/JWT validation on either gRPC service.** Keycloak/OIDC authenticates human operators at the Admin API (EP-004) — a separate concern from this machine link.

## Decision

Authenticate the gateway→Building OS gRPC links with **mutual TLS terminated at the Building OS Envoy edge**, not with OIDC bearer tokens. Concretely:

1. **Production:** the gateway presents a **client certificate** whose CN/SAN encodes its `gateway_id`. The Envoy (or Contour/Emissary) ingress requires the client cert, terminates mTLS, and forwards h2c to ConnectorWorker/GatewayBridge. Certificate issuance/rotation is **cert-manager**-operated on the Building OS side; the gateway is provisioned with its cert/key + the trust CA. The `gateway_id ↔ cert CN/SAN` binding is the authentication that `GatewayIngressService`'s ownership check assumes.
2. **In-cluster / behind the edge:** plaintext h2c is accepted — the gRPC apps never speak TLS themselves.
3. **The gateway is config-driven and default-secure:** uplink and egress take a CA bundle and an optional client cert/key. With them set, the link is TLS/mTLS. A clear-text h2c path is available **only** behind an explicit `--insecure`/dev flag for local and CI runs where no edge proxy exists (EP-009/EP-010 today run GatewayIngress/GatewayBridge directly with no Envoy). `insecure.NewCredentials()` is removed from the default build path.
4. **No bearer token on the gRPC metadata** for these links — Building OS does not validate one, so adding it would be security theater. Authorization remains Building OS's `gateway_id`-ownership check.

## Consequences

- Matches Building OS's stated and partially-implemented model (#161); the gateway does not invent a second auth scheme. The Keycloak/OIDC machinery already in the repo stays scoped to the Admin API.
- Production readiness depends on Building OS operational pieces the gateway does **not** own: Envoy client-cert enforcement, cert-manager issuance, and the `gateway_id ↔ CN/SAN` mapping. Whether the Helm/Envoy config is fully implemented on the Building OS side is **unconfirmed** and must be tracked as an external dependency (Building OS #161) before claiming production mTLS.
- Until the edge exists in dev/CI, EP-009/EP-010 E2E runs plaintext h2c against the gRPC services directly; the test topology must expose ConnectorWorker's gRPC ingress port (internal-only by default) and GatewayBridge's egress port.
- The gateway must treat a TLS/cert error as a connection failure that the Store-and-Forward buffer rides out (ADR-0002), never a crash-loop or silent drop.
- If Building OS later adds in-app token validation, this ADR is revisited; the config-driven credential surface (CA + client cert) is forward-compatible with adding a token source without re-architecting.
