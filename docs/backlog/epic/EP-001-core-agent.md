# EP-001: Core Agent & Connector Lifecycle

**Status:** Draft
**Priority:** P0

## Goal

The Core Agent is the Go orchestration brain of the gateway. It manages connector containers (start/stop/restart/upgrade), holds configuration, monitors health, and exposes the Admin API. Without it there is no way to operate or observe the gateway, so it is the foundation of MVP-1.

## Acceptance Criteria

- [ ] Core Agent runs as a single Go binary and manages connector containers via the Docker Engine SDK.
- [ ] Connector Registry tracks installed connectors and their versions.
- [ ] Lifecycle Manager supports Start, Stop, Restart, and Upgrade of connectors.
- [ ] Config Manager persists and distributes gateway + connector configuration.
- [ ] Health Monitor reports gateway uptime, CPU, memory, and disk.
- [ ] Admin API exposes the above operations, protected by Keycloak OIDC/OAuth2.

## Child Features

- [ ] FEAT-001: Connector Registry
- [ ] FEAT-002: Lifecycle Manager (Docker SDK)
- [ ] FEAT-003: Config Manager
- [ ] FEAT-004: Health Monitor
- [ ] FEAT-005: Admin API + Keycloak auth
