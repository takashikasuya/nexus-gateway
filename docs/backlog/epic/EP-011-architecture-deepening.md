# EP-011: Architecture Deepening & Building OS #224 Alignment

**Status:** Proposed
**Priority:** P1 (FEAT-031 carries a P0 correctness gap — see Dependencies)

## Goal

Turn shallow, copy-pasted, or untestable seams into **deep modules** — high leverage behind a small interface, with the interface as the test surface. The behaviors that the ADRs describe (best-effort store-and-forward, the Normalizer as the single home of identity/semantic mapping, the connector SDK with a bounded ack window, the Point List as one synced concept) currently live as emergent properties of `cmd/gateway/main.go` wiring or as logic welded to NATS/gRPC. This epic concentrates each into one owned, tested module. No behavior change to the wire contract; the gateway↔Building OS gRPC contract stays stable.

This epic also closes a concrete gap surfaced while reviewing `gutp-building-os-oss` #224 (now merged, PR #253/#255/#257): the real gateway-scoped Point List API exists and is **ETag-based** (`GET /gateways/{gatewayId}/pointlist`, `If-None-Match`/304, `?since={etag}` diff, mTLS-derived identity), and Building OS now pushes `EgressDown.point_list_update`. The gateway's provisioning client and egress proto were written against an imagined `/version`+`/snapshot` shape and do not yet match.

## Acceptance Criteria

- [ ] Each deepened module has a small interface that is also its unit-test surface — the targeted behavior is testable in-process, without standing up a live NATS/gRPC/Building OS stack.
- [ ] No regression in the existing live-stack E2E suite (`test/e2e/`, `integration/`).
- [ ] No breaking change to `proto/` (Buf breaking-change detection passes); `EgressDown` only gains the additive `point_list_update` field already present on the Building OS side.
- [ ] The gateway's HTTP provisioning client speaks the real #224 contract (ETag/304/`?since=`), and the gateway acts on `point_list_update` push by revalidating.

## Child Features

- [ ] **FEAT-028: Connector SDK + shared wire contract** (deepens EP-002, fulfils ADR-0005's SDK clause).
  The Command Channel `control_id` dedup + in-flight sentinel + bounded-ack-window publish are currently re-implemented three times (Go/Python/Java) and already diverging (MQTT: no eviction, replies `in_flight`; BACnet/OPC-UA: 1000-entry evict, silent drop). The wire types (`CommonEvent`, `ControlCommand`, `WriteReply`) are redefined per language with nothing enforcing agreement. Make the Connector↔Gateway internal protocol one generated contract (protobuf or single JSON schema) and give each language a thin SDK owning publish-with-ack-window + dedup behind a small interface, so a Connector carries only protocol-specific code.

- [ ] **FEAT-029: Store-and-Forward delivery policy as a tested module** (deepens EP-003, makes ADR-0002 testable).
  The ADR-0002 rules — advance cursor on `StreamAck.accepted`, record `accepted < sent` as a per-`point_id` drift counter, replay the whole un-acked batch on pre-ack failure, never resend rejects — live entirely inside the untested 95-line `internal/uplink/ingress.go` `runStream`, fused with gRPC stream open/close and two timers. Extract the checkpoint/advance/drift decision behind a small interface over the `storeforward.Buffer` and an injected "send batch → accepted-count" seam; gRPC streaming becomes an adapter at that seam.

- [ ] **FEAT-030: Normalizer Common Event → Telemetry decision as a pure module** (deepens EP-003, makes ADR-0001 explicit).
  The identity/semantic mapping (decode, resolve `local_id`→`point_id`, classify `ok`/`poison`/`miss`, coerce `value` to numeric per CONTEXT.md) is a private function + private enum welded to JetStream `Fetch`/`Ack`/`Term`; it is testable today only via 500 ms channel-timeout inference and forces serial tests through process-global counters. Extract `(Common Event, Resolver) → Outcome{frame|poison|miss}` as a pure module; the JetStream consumer becomes a thin adapter mapping outcome → Ack/Term + counter.

- [ ] **FEAT-031: Point List as one deep module, aligned to the real #224 API** (deepens EP-006). **Carries a P0 correctness gap.**
  Today "the Point List" is split across `pointlist.SyncedResolver` (atomic swap/persist), `pointsync.Loop` (poll cadence), `dispatch`'s separately-redeclared reverse `Resolver` interface, and `main.go` (initial Load + blocking first sync) — no module owns convergence. Give it one owning module whose interface is the convergence lifecycle (file bootstrap → provisioning sync override → blocking-first-load → forward + reverse resolution). **Within this, replace the imagined `/version`+`/snapshot` `HTTPClient` with the real #224 ETag contract** (`GET /gateways/{gatewayId}/pointlist`, `If-None-Match`/304, `?since={etag}` diff with full-fallback, mTLS-header identity), and **handle `EgressDown.point_list_update`** as a revalidation hint. The current client targets endpoints that do not exist on Building OS — this is a P0 fix, not just a refactor.

## Dependencies

- **Cross-repo (now satisfied):** `gutp-building-os-oss` #224 is merged. The real contract is: `GET /gateways/{gatewayId}/pointlist` (ETag/304/`?since=`), `EgressDown.point_list_update` push, mTLS-derived `gateway_id` header binding. FEAT-031 implements against this, not the fixture-file placeholder EP-006 assumed.
- **ADRs (fulfilled, not re-litigated):** FEAT-028→ADR-0005 (connector SDK clause); FEAT-029→ADR-0002; FEAT-030→ADR-0001; FEAT-031→ADR-0003 + ADR-0007 (mTLS identity).
- **Consequence, not a candidate:** `cmd/gateway/main.go` (288 lines, seam-less manual wiring) shrinks as 028–031 land; it is fixed incrementally, not as its own feature.
