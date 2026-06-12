# EP-002: Protocol Connectors

**Status:** Draft
**Priority:** P0

## Goal

Connectors are independent per-protocol containers that talk to field equipment and emit the common event format onto NATS JetStream. They absorb protocol diversity at the edge. MVP-1 requires BACnet, OPC-UA, and MQTT; Modbus and future protocols follow the same container pattern.

## Acceptance Criteria

- [ ] Each connector is an isolated container with no dependency on other connectors and no Building-OS-specific or equipment-specific domain model.
- [ ] Every connector emits the common event format (gateway_id, protocol, device_id, point_id, value, unit, quality, timestamp) to NATS JetStream.
- [ ] BACnet connector (Python, BACpypes3/BAC0) supports Who-Is, I-Am, ReadProperty, ReadPropertyMultiple, WriteProperty, WritePropertyMultiple, SubscribeCOV.
- [ ] OPC-UA connector (Java, Eclipse Milo) supports Browse, Read, Write, Subscribe, Method Call.
- [ ] MQTT connector (Go or Python) supports MQTT 5.0.
- [ ] (Post-MVP) Modbus connector (Go or Python) supports Modbus TCP.

## Child Features

- [ ] FEAT-006: Connector common event SDK / contract
- [ ] FEAT-007: BACnet connector
- [ ] FEAT-008: OPC-UA connector
- [ ] FEAT-009: MQTT connector
- [ ] FEAT-010: Modbus connector (post-MVP)
