"""OPC-UA server simulator — config-driven.

Reads node definitions from a YAML file (fixtures/scale/sim-config/opcua-01.yml)
so the point list remains the single source of truth.

YAML schema:
  connector_id: opcua-01
  endpoint: "opc.tcp://0.0.0.0:4840/"
  namespace_uri: "http://nexus-gateway.io/sim"
  value_change_rate: 5
  nodes:
    - local_id: "ns=2;s=UA-F-0001"
      type: Float    # Float | Bool | Int32
      unit: degC
      writable: true
    ...

Environment:
  OPCUA_SIM_CONFIG       — path to YAML config file (default: auto-detect)
  OPCUA_SERVER_ENDPOINT  — overrides endpoint in config
  VALUE_CHANGE_RATE      — overrides value_change_rate in config
"""
from __future__ import annotations

import asyncio
import math
import os
import sys
import time
from pathlib import Path

import yaml
from asyncua import Server, ua

_SENTINEL = object()


def _load_config() -> dict:
    config_path = os.environ.get("OPCUA_SIM_CONFIG", "")
    if not config_path:
        candidates = [
            Path(__file__).parent.parent.parent.parent
            / "fixtures/scale/sim-config/opcua-01.yml",
            Path.cwd() / "fixtures/scale/sim-config/opcua-01.yml",
        ]
        for c in candidates:
            if c.exists():
                config_path = str(c)
                break
    if not config_path:
        print("ERROR: no OPCUA_SIM_CONFIG found")
        sys.exit(1)
    print(f"  loaded: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)


async def main() -> None:
    print("OPC-UA simulator (config-driven)")
    cfg = _load_config()

    endpoint = os.environ.get("OPCUA_SERVER_ENDPOINT") or cfg.get(
        "endpoint", "opc.tcp://0.0.0.0:4840/"
    )
    rate = float(os.environ.get("VALUE_CHANGE_RATE") or cfg.get("value_change_rate", 5))
    ns_uri = cfg.get("namespace_uri", "http://nexus-gateway.io/sim")
    nodes_cfg: list[dict] = cfg.get("nodes", [])

    server = Server()
    await server.init()
    server.set_endpoint(endpoint)
    server.set_server_name(
        f"nexus-gateway OPC-UA sim — {cfg.get('connector_id', 'opcua')} "
        f"({len(nodes_cfg)} nodes)"
    )

    nsidx = await server.register_namespace(ns_uri)
    objects = server.nodes.objects
    device = await objects.add_object(nsidx, "SimDevice")

    float_nodes: list = []
    bool_nodes: list = []
    int_nodes: list = []

    for node_cfg in nodes_cfg:
        lid: str = node_cfg["local_id"]          # ns=2;s=UA-F-0001
        name = lid.split(";s=")[1]               # UA-F-0001
        ntype: str = node_cfg.get("type", "Float")

        if ntype == "Float":
            node = await device.add_variable(nsidx, name, float(20.0))
        elif ntype == "Bool":
            node = await device.add_variable(nsidx, name, False)
        else:  # Int32
            node = await device.add_variable(
                nsidx, name, ua.Variant(0, ua.VariantType.Int32)
            )

        if node_cfg.get("writable", False):
            await node.set_writable()

        if ntype == "Float":
            float_nodes.append(node)
        elif ntype == "Bool":
            bool_nodes.append(node)
        else:
            int_nodes.append(node)

    print(
        f"  endpoint={endpoint}  namespace={nsidx}  "
        f"Float={len(float_nodes)}  Bool={len(bool_nodes)}  Int={len(int_nodes)}  "
        f"rate={rate}s"
    )

    async with server:
        start = time.time()
        while True:
            elapsed = time.time() - start

            for idx, node in enumerate(float_nodes):
                phase = 2 * math.pi * idx / max(len(float_nodes), 1)
                value = 20.0 + 5.0 * math.sin(2 * math.pi * elapsed / 60.0 + phase)
                await node.write_value(round(value, 2))

            for idx, node in enumerate(bool_nodes):
                period = 30.0 + idx * 0.1
                await node.write_value(int(elapsed / period) % 2 == 0)

            for idx, node in enumerate(int_nodes):
                await node.write_value(
                    ua.Variant(int(elapsed + idx) % 256, ua.VariantType.Int32)
                )

            await asyncio.sleep(rate)


if __name__ == "__main__":
    asyncio.run(main())
