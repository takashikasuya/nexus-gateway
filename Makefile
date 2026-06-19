GOBIN ?= $(HOME)/go/bin
GO    ?= go
BUF   ?= $(GOBIN)/buf

# Absolute paths are required for docker compose -f to avoid Docker Compose v5
# treating pyproject.toml-bearing build contexts (connector/bacnet) as sub-projects.
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
COMPOSE_BASE := -f $(ROOT)/docker-compose.yml -f $(ROOT)/docker-compose.integration.yml

OPCUA_ENDPOINT  ?= opc.tcp://192.168.0.26:4840
BACNET_ADDRESS  ?= 192.168.0.26

.PHONY: generate build test lint clean \
        e2e-up-opcua e2e-up-bacnet e2e-up-both e2e-down \
        e2e-opcua e2e-bacnet e2e-both

generate:
	$(BUF) generate

build: generate
	$(GO) build ./...

test:
	$(GO) test -timeout 60s ./...

buf-breaking:
	$(BUF) breaking --against '.git#branch=master,subdir=proto'

clean:
	rm -f gen/*.go

# ── E2E integration targets (remote devices at 192.168.0.26) ─────────────────
# Override OPCUA_ENDPOINT / BACNET_ADDRESS if devices are at a different address.

e2e-up-opcua:
	OPCUA_ENDPOINT=$(OPCUA_ENDPOINT) \
	  docker compose $(COMPOSE_BASE) --profile opcua-remote up -d --no-build

e2e-up-bacnet:
	BACNET_ADDRESS=$(BACNET_ADDRESS) \
	  docker compose $(COMPOSE_BASE) --profile bacnet-remote up -d --no-build

e2e-up-both:
	OPCUA_ENDPOINT=$(OPCUA_ENDPOINT) BACNET_ADDRESS=$(BACNET_ADDRESS) \
	  docker compose $(COMPOSE_BASE) --profile opcua-remote --profile bacnet-remote up -d --no-build

e2e-down:
	docker compose $(COMPOSE_BASE) --profile opcua-remote --profile bacnet-remote down

e2e-opcua: e2e-up-opcua
	docker run --rm --network host \
	  -v $(ROOT):/workspace -w /workspace \
	  -e E2E_NATS_URL=nats://localhost:14222 \
	  -e E2E_ADMIN_URL=http://localhost:18080 \
	  golang:1.25-alpine \
	  go test ./integration/... -run 'TestE2E_(OpcUATelemetry|OpcUAControl)' -v -timeout 120s

e2e-bacnet: e2e-up-bacnet
	docker run --rm --network host \
	  -v $(ROOT):/workspace -w /workspace \
	  -e E2E_NATS_URL=nats://localhost:14222 \
	  golang:1.25-alpine \
	  go test ./integration/... -run 'TestE2E_(BacnetTelemetry|BacnetControl)' -v -timeout 120s

e2e-both: e2e-up-both
	docker run --rm --network host \
	  -v $(ROOT):/workspace -w /workspace \
	  -e E2E_NATS_URL=nats://localhost:14222 \
	  -e E2E_ADMIN_URL=http://localhost:18080 \
	  golang:1.25-alpine \
	  go test ./integration/... \
	  -run 'TestE2E_(OpcUATelemetry|BacnetTelemetry|OpcUAControl|BacnetControl)' \
	  -v -timeout 180s
