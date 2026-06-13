"""Minimal OPC-UA server simulator: sine-wave temperature and a boolean status node."""
from __future__ import annotations

import asyncio
import math
import os
import time

from asyncua import Server
from asyncua.ua import NodeId, ObjectIds


ENDPOINT = os.environ.get("OPCUA_SERVER_ENDPOINT", "opc.tcp://0.0.0.0:4840/freeopcua/server/")
NAMESPACE = "http://nexus-gateway.io/sim"


async def main() -> None:
    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)
    server.set_server_name("nexus-gateway OPC-UA simulator")

    nsidx = await server.register_namespace(NAMESPACE)

    objects = server.nodes.objects
    device = await objects.add_object(nsidx, "SimDevice")

    temp_node = await device.add_variable(nsidx, "Temperature", 20.0)
    await temp_node.set_writable()

    status_node = await device.add_variable(nsidx, "Active", True)
    await status_node.set_writable()

    temp_id = (await temp_node.get_node_id()).to_string()
    status_id = (await status_node.get_node_id()).to_string()
    print(f"OPC-UA simulator running: endpoint={ENDPOINT}")
    print(f"  Temperature node: {temp_id}")
    print(f"  Active node:      {status_id}")

    async with server:
        start = time.time()
        while True:
            elapsed = time.time() - start
            temp = 20.0 + 5.0 * math.sin(2 * math.pi * elapsed / 60.0)
            await temp_node.write_value(round(temp, 2))
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
