"""Minimal BACnet/IP device simulator: one analogInput with a sine-wave present value."""
from __future__ import annotations

import asyncio
import math
import os
import time

from bacpypes3.app import Application
from bacpypes3.local.analog import AnalogInputObject
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import Real


DEVICE_ID = int(os.environ.get("BACNET_DEVICE_ID", "999"))
BACNET_ADDRESS = os.environ.get("BACNET_ADDRESS", "0.0.0.0")


async def main() -> None:
    device = DeviceObject(
        objectIdentifier=("device", DEVICE_ID),
        objectName="bacnet-sim",
        description="nexus-gateway BACnet simulator",
        vendorIdentifier=999,
    )
    app = Application(device, Address(f"{BACNET_ADDRESS}/24"))

    sensor = AnalogInputObject(
        objectIdentifier=("analogInput", 0),
        objectName="temperature-0",
        description="simulated temperature sensor",
        units="degC",
        presentValue=Real(20.0),
    )
    app.add_object(sensor)

    print(f"BACnet simulator running: device {DEVICE_ID} at {BACNET_ADDRESS}")
    start = time.time()
    while True:
        elapsed = time.time() - start
        # Sine wave between 15 and 25 degC with a 60-second period.
        value = 20.0 + 5.0 * math.sin(2 * math.pi * elapsed / 60.0)
        sensor.presentValue = Real(round(value, 2))
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
