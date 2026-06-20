"""BACnet/IP device simulator with 1000 objects for scale testing.

Object distribution:
  analogInput  × 500  (instances  1-500)  — sine-wave with per-point phase offset
  analogOutput × 200  (instances  1-200)  — writable setpoints (initial value 20.0)
  binaryInput  × 200  (instances  1-200)  — alternating True/False every 30 s
  binaryOutput × 100  (instances  1-100)  — writable commands (initial value False)

Environment variables:
  BACNET_DEVICE_ID   — BACnet device object-identifier (default: 1001)
  BACNET_ADDRESS     — local interface address (default: 0.0.0.0)
  VALUE_CHANGE_RATE  — seconds between analog value updates (default: 5)
"""
from __future__ import annotations

import asyncio
import math
import os
import time

from bacpypes3.app import Application
from bacpypes3.local.analog import AnalogInputObject, AnalogOutputObject
from bacpypes3.local.binary import BinaryInputObject, BinaryOutputObject
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import Real

DEVICE_ID = int(os.environ.get("BACNET_DEVICE_ID", "1001"))
BACNET_ADDRESS = os.environ.get("BACNET_ADDRESS", "0.0.0.0")
VALUE_CHANGE_RATE = float(os.environ.get("VALUE_CHANGE_RATE", "5"))

# Wave period per object type to spread updates
AI_PERIOD = 60.0   # seconds for full sine cycle


async def main() -> None:
    device = DeviceObject(
        objectIdentifier=("device", DEVICE_ID),
        objectName=f"bacnet-sim-scale-{DEVICE_ID}",
        description="nexus-gateway BACnet scale simulator (1000 objects)",
        vendorIdentifier=999,
    )
    app = Application(device, Address(f"{BACNET_ADDRESS}/24"))

    # analogInput × 500
    ai_objects: list[AnalogInputObject] = []
    for i in range(1, 501):
        obj = AnalogInputObject(
            objectIdentifier=("analogInput", i),
            objectName=f"ai-{i:04d}",
            description=f"Analog input {i}",
            units="degC",
            presentValue=Real(20.0),
        )
        app.add_object(obj)
        ai_objects.append(obj)

    # analogOutput × 200
    ao_objects: list[AnalogOutputObject] = []
    for i in range(1, 201):
        obj = AnalogOutputObject(
            objectIdentifier=("analogOutput", i),
            objectName=f"ao-{i:04d}",
            description=f"Analog output {i}",
            units="degC",
            presentValue=Real(20.0),
        )
        app.add_object(obj)
        ao_objects.append(obj)

    # binaryInput × 200
    bi_objects: list[BinaryInputObject] = []
    for i in range(1, 201):
        obj = BinaryInputObject(
            objectIdentifier=("binaryInput", i),
            objectName=f"bi-{i:04d}",
            description=f"Binary input {i}",
            presentValue="inactive",
        )
        app.add_object(obj)
        bi_objects.append(obj)

    # binaryOutput × 100
    bo_objects: list[BinaryOutputObject] = []
    for i in range(1, 101):
        obj = BinaryOutputObject(
            objectIdentifier=("binaryOutput", i),
            objectName=f"bo-{i:04d}",
            description=f"Binary output {i}",
            presentValue="inactive",
        )
        app.add_object(obj)
        bo_objects.append(obj)

    total = len(ai_objects) + len(ao_objects) + len(bi_objects) + len(bo_objects)
    print(
        f"BACnet scale simulator: device {DEVICE_ID} at {BACNET_ADDRESS}/24  "
        f"total={total} objects "
        f"(AI={len(ai_objects)}, AO={len(ao_objects)}, "
        f"BI={len(bi_objects)}, BO={len(bo_objects)})"
    )
    print(f"  Value update rate: every {VALUE_CHANGE_RATE}s")

    start = time.time()
    while True:
        elapsed = time.time() - start

        # Update analogInput: each point has a unique phase offset so they
        # don't all peak at the same time (avoids NATS burst storm).
        for idx, obj in enumerate(ai_objects):
            phase = 2 * math.pi * idx / len(ai_objects)
            value = 20.0 + 5.0 * math.sin(2 * math.pi * elapsed / AI_PERIOD + phase)
            obj.presentValue = Real(round(value, 2))

        # Update binaryInput: flip every 30 s, staggered by index
        for idx, obj in enumerate(bi_objects):
            flip_period = 30.0 + idx * 0.1   # slight stagger
            obj.presentValue = "active" if int(elapsed / flip_period) % 2 == 0 else "inactive"

        await asyncio.sleep(VALUE_CHANGE_RATE)


if __name__ == "__main__":
    asyncio.run(main())
