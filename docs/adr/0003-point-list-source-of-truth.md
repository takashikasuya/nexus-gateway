# Point List source of truth is the Building OS twin; the gateway syncs by diff

**Status:** accepted — 2026-06-13

The shared Point List (maps `point_id` ⇄ native addressing, unit, writeability, control schema, device/spatial grouping) has its **single source of truth in the Building OS twin** (OxiGraph `sbco:PointExt`). The gateway holds a *synced copy*: it polls a cheap version token, fetches the authoritative snapshot when it changes, **diffs** against its local copy, and reconverges the Normalizer mapping and Connector poll lists. The gateway never authors the Point List.

We chose Building-OS-as-master because the contract is point-id based (#181) and the divergence between the gateway's copy and the twin is exactly what surfaces as `accepted < sent` telemetry drift (see [0002](./0002-best-effort-store-and-forward.md)). A single authoritative copy with one-directional sync removes the ambiguity of reconciling two editable masters.

## Consequences

- Requires a **new gateway-scoped, versioned Point List provisioning API** on Building OS — the existing `GET /points` is user-RBAC-scoped and omits native addressing. Filed as `gutp-building-os-oss` issue #224.
- Open dependency: whether native addressing (BACnet object/instance, etc.) lives in the twin or in the `smartbuilding_datamodel_builder` CSV must be resolved by Building OS; if Building OS is master, its export must include native addressing. Tracked in #224.
- Local device discovery on the gateway feeds *proposals* into the Point List authoring flow (datamodel builder), not the live twin directly.
