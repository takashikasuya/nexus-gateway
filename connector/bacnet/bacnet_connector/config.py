"""Configuration loaded from environment variables."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


@dataclass
class PointConfig:
    local_id: str        # BACnet native address: "<objtype>,<instance>" e.g. "analogInput,0"
    device_ref: str      # opaque device reference for the Normalizer
    unit: str = ""       # engineering unit, e.g. "degC"
    writable: bool = False


@dataclass
class Config:
    connector_id: str
    nats_url: str
    bacnet_address: str  # target BACnet device IP address or "host/24"
    bacnet_device_id: int
    points: list[PointConfig] = field(default_factory=list)
    poll_interval: float = 30.0  # seconds between full polls
    local_address: str = "0.0.0.0"  # local BACnet interface address

    @classmethod
    def from_env(cls) -> "Config":
        points_raw = os.environ.get("BACNET_POINTS", "[]")
        points = [
            PointConfig(
                local_id=p["local_id"],
                device_ref=p.get("device_ref", ""),
                unit=p.get("unit", ""),
                writable=p.get("writable", False),
            )
            for p in json.loads(points_raw)
        ]
        return cls(
            connector_id=os.environ["CONNECTOR_ID"],
            nats_url=os.environ.get("NATS_URL", "nats://localhost:4222"),
            bacnet_address=os.environ["BACNET_ADDRESS"],
            bacnet_device_id=int(os.environ["BACNET_DEVICE_ID"]),
            points=points,
            poll_interval=float(os.environ.get("BACNET_POLL_INTERVAL", "30")),
            local_address=os.environ.get("BACNET_LOCAL_ADDRESS", "0.0.0.0"),
        )
