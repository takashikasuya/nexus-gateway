# Telemetry pipeline: JetStream before the Normalizer, Normalizer owns identity

**Status:** accepted — 2026-06-13

The telemetry path is **Connectors → NATS JetStream → Normalizer → Store-and-Forward → gRPC Ingress Uplink**. Connectors publish raw, protocol-tagged Common Events carrying *native* addressing only (`local_id`, native device ref); they hold no canonical identity and no domain model. The Normalizer is the single JetStream consumer that joins the shared Point List to resolve the canonical `point_id`, unifies unit/quality/timestamp, and produces the `TelemetryFrame`.

We place JetStream **before** normalization (not after) so that raw Common Events are durable and **replayable**: when the Point List changes (a `local_id`→`point_id` remap, a unit change), we can re-run the Normalizer over retained raw events and get corrected Telemetry. If Connectors baked canonical `point_id` into the event, that identity would be frozen in the stream and replay could not fix the exact change it exists to absorb.

## Consequences

- The Normalizer is the *only* place identity/semantic mapping lives, matching "mapping never in Connectors." Point List churn reloads one component, not every Connector.
- Connectors still read the Point List — but only for the native addresses to poll/subscribe; they ignore the canonical columns. Minor redundant distribution, accepted for decoupling.
- Wire identity is `(gateway_id, point_id)` only; `device_id`/`building_id` are twin-derived and not sent.
