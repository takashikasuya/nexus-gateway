# EP-008: Edge Hardening & Operations

**Status:** Prod
**Priority:** P1 (security items P0 before any non-PoC deployment)

## Goal

The pipeline is a working walking skeleton, but several seams are PoC-grade and must be hardened before the gateway runs outside a lab: the northbound/southbound gRPC links are unauthenticated and unencrypted, the in-process sim Connector violates the connector-isolation invariant (ADR-0001), the Normalizer silently drops Telemetry whose `local_id` is not yet in the Point List, and there is no operator-facing run book. This epic closes those gaps so the system is **operable, observable, and secure-by-default** without changing the pipeline shape.

This epic does **not** introduce new pipeline stages; it hardens existing ones. Connector distribution/signing is EP-007; this epic stops at how the gateway *runs and connects*.

## Acceptance Criteria

- [ ] The Ingress uplink (`internal/uplink`) and the Egress agent (`internal/egress`) connect to Building OS over TLS with a verified server certificate; `insecure.NewCredentials()` is gone from non-test code.
- [ ] Each gRPC call to Building OS carries a gateway service credential (OIDC client-credentials token or mTLS client cert) that Building OS can authenticate to the issuing `gateway_id`; the mechanism is fixed in **ADR-0007 (transport security)** before implementation.
- [ ] Transport security degrades safely: a cert/token error surfaces as a connection failure that the Store-and-Forward buffer rides out (ADR-0002), never as silent data loss or a crash loop without backoff.
- [ ] The Normalizer's behavior for a Common Event whose `local_id` is absent from the current Point List is an explicit, documented policy fixed in **ADR-0008 (point-list-miss policy)** — not the current implicit "Nak ×3 then drop". The chosen policy is implemented with a counter/metric so misses are observable.
- [ ] The sim Connector no longer runs in-process inside `cmd/gateway`; it is either a separate container started through the lifecycle Manager or gated behind an explicit `--dev-sim` flag that is off by default and documented as non-production. The connector-isolation invariant (ADR-0001) holds in the default build.
- [ ] `go.mod` pins a toolchain version that CI actually runs; the `go` directive matches a released stable Go and `go build`/`go test` pass on that version in CI.
- [ ] A top-level `README` documents: what the gateway is, the pipeline diagram, prerequisites, `docker compose up` quickstart, the env/flags surface (NATS, BOS, Keycloak, Admin API), and how to point connectors at the simulators (EP-009).
- [ ] `docker compose up` from a clean checkout brings the full stack (NATS, mock/real Building OS, gateway, Keycloak, Admin UI) to healthy with documented health checks; no manual post-steps.

## Child Features

- [ ] FEAT-032: gRPC transport security — TLS + service credential for Ingress uplink and Egress agent (ADR-0007). **HITL** (cross-repo contract with Building OS; new ADR).
- [ ] FEAT-033: Point-List-miss policy for the Normalizer — park/hold/drop decision, metric, bounded behavior (ADR-0008). **HITL** (new ADR).
- [ ] FEAT-034: Sim Connector externalization — remove in-process sim from `cmd/gateway`; run as container or dev-only flag (ADR-0001 isolation).
- [ ] FEAT-035: Toolchain & dependency hygiene — pin `go` directive to CI's stable Go, tidy modules, green CI.
- [ ] FEAT-036: Operator run book & compose hardening — README, quickstart, env/flags reference, health-checked one-command bring-up.

## Dependencies

- EP-001 Core Agent — FEAT-034 reuses the Docker SDK lifecycle Manager to host the externalized sim.
- EP-003 Normalizer/Uplink — FEAT-032 and FEAT-033 modify these stages.
- EP-005 Control Path — FEAT-032 also secures the Egress gRPC link used by the Command path.
- **External:** Building OS (`gutp-building-os-oss`) must accept the chosen TLS + credential mechanism; FEAT-032 is blocked on that contract being agreed in ADR-0007.
- **Pending decisions:** ADR-0007 (transport security: mTLS vs OIDC client-credentials vs both) and ADR-0008 (point-list-miss policy) are not yet written; the two HITL features gate on them.
