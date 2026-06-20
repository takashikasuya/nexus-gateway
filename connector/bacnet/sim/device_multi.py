"""Multi-device BACnet/IP simulator — config-driven.

Reads one YAML file per virtual device. By default loads all bacnet-XX.yml
files from the directory pointed to by BACNET_SIM_CONFIG_DIR, or individual
files listed in BACNET_SIM_CONFIGS (comma-separated paths).

YAML schema (fixtures/scale/sim-config/bacnet-XX.yml):
  connector_id: bacnet-01
  device_id:    1001
  address:      "0.0.0.0:47808"   # local bind address for this virtual device
  device_ref:   ahu-01
  value_change_rate: 5             # seconds between analog updates
  objects:
    - local_id: "analogInput,1"
      unit: degC
      writable: false
    - local_id: "binaryInput,1"
      writable: false
    ...

Environment:
  BACNET_SIM_CONFIG_DIR  — directory containing bacnet-*.yml (default: auto-detect)
  BACNET_SIM_CONFIGS     — comma-separated explicit YAML paths (overrides DIR)
  VALUE_CHANGE_RATE      — global override for value_change_rate in all configs
"""
from __future__ import annotations

import asyncio
import glob
import math
import os
import sys
import time
from pathlib import Path

import yaml
from bacpypes3.app import Application
from bacpypes3.local.analog import AnalogInputObject, AnalogOutputObject
from bacpypes3.local.binary import BinaryInputObject, BinaryOutputObject
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import Real

AI_PERIOD = 60.0  # sine-wave period in seconds


def _load_configs() -> list[dict]:
    explicit = os.environ.get("BACNET_SIM_CONFIGS", "")
    if explicit:
        paths = [p.strip() for p in explicit.split(",") if p.strip()]
    else:
        config_dir = os.environ.get("BACNET_SIM_CONFIG_DIR", "")
        if not config_dir:
            # Auto-detect: look next to this script, then CWD
            candidates = [
                Path(__file__).parent.parent.parent.parent
                / "fixtures/scale/sim-config",
                Path.cwd() / "fixtures/scale/sim-config",
            ]
            for c in candidates:
                if c.is_dir():
                    config_dir = str(c)
                    break
        if not config_dir:
            print("ERROR: no BACNET_SIM_CONFIG_DIR found and BACNET_SIM_CONFIGS not set")
            sys.exit(1)
        paths = sorted(glob.glob(os.path.join(config_dir, "bacnet-*.yml")))

    if not paths:
        print("ERROR: no bacnet-*.yml config files found")
        sys.exit(1)

    configs = []
    for p in paths:
        with open(p) as f:
            configs.append(yaml.safe_load(f))
        print(f"  loaded: {p}")
    return configs


def _build_app(cfg: dict) -> tuple[Application, list, list, list, list]:
    device = DeviceObject(
        objectIdentifier=("device", cfg["device_id"]),
        objectName=cfg.get("device_ref", f"device-{cfg['device_id']}"),
        description=f"nexus-gateway BACnet sim — {cfg.get('connector_id', '')}",
        vendorIdentifier=999,
    )
    app = Application(device, Address(cfg["address"]))

    ai_objs, ao_objs, bi_objs, bo_objs = [], [], [], []

    for obj_cfg in cfg.get("objects", []):
        lid: str = obj_cfg["local_id"]
        obj_type, _, inst_str = lid.partition(",")
        inst = int(inst_str)

        if obj_type == "analogInput":
            o = AnalogInputObject(
                objectIdentifier=("analogInput", inst),
                objectName=f"ai-{inst:03d}",
                units=obj_cfg.get("unit", "noUnits"),
                presentValue=Real(20.0),
            )
            app.add_object(o)
            ai_objs.append(o)

        elif obj_type == "analogOutput":
            o = AnalogOutputObject(
                objectIdentifier=("analogOutput", inst),
                objectName=f"ao-{inst:03d}",
                units=obj_cfg.get("unit", "noUnits"),
                presentValue=Real(20.0),
            )
            app.add_object(o)
            ao_objs.append(o)

        elif obj_type == "binaryInput":
            o = BinaryInputObject(
                objectIdentifier=("binaryInput", inst),
                objectName=f"bi-{inst:03d}",
                presentValue="inactive",
            )
            app.add_object(o)
            bi_objs.append(o)

        elif obj_type == "binaryOutput":
            o = BinaryOutputObject(
                objectIdentifier=("binaryOutput", inst),
                objectName=f"bo-{inst:03d}",
                presentValue="inactive",
            )
            app.add_object(o)
            bo_objs.append(o)

    return app, ai_objs, ao_objs, bi_objs, bo_objs


async def _update_loop(
    device_idx: int,
    total_devices: int,
    rate: float,
    ai_objs: list,
    bi_objs: list,
) -> None:
    start = time.time()
    phase_base = 2 * math.pi * device_idx / total_devices
    while True:
        elapsed = time.time() - start
        for idx, obj in enumerate(ai_objs):
            phase = phase_base + 2 * math.pi * idx / max(len(ai_objs), 1)
            value = 20.0 + 5.0 * math.sin(2 * math.pi * elapsed / AI_PERIOD + phase)
            obj.presentValue = Real(round(value, 2))
        for idx, obj in enumerate(bi_objs):
            period = 30.0 + device_idx * 2.0 + idx * 0.1
            obj.presentValue = "active" if int(elapsed / period) % 2 == 0 else "inactive"
        await asyncio.sleep(rate)


async def main() -> None:
    print("BACnet multi-device simulator (config-driven)")
    configs = _load_configs()

    global_rate = os.environ.get("VALUE_CHANGE_RATE")
    apps, tasks = [], []

    for idx, cfg in enumerate(configs):
        rate = float(global_rate or cfg.get("value_change_rate", 5))
        app, ai_objs, ao_objs, bi_objs, bo_objs = _build_app(cfg)
        apps.append(app)
        total = len(ai_objs) + len(ao_objs) + len(bi_objs) + len(bo_objs)
        print(
            f"  {cfg.get('connector_id', cfg['device_id'])} "
            f"→ device {cfg['device_id']} @ {cfg['address']}  "
            f"objects={total} (AI={len(ai_objs)}, AO={len(ao_objs)}, "
            f"BI={len(bi_objs)}, BO={len(bo_objs)})  rate={rate}s"
        )
        tasks.append(
            asyncio.create_task(
                _update_loop(idx, len(configs), rate, ai_objs, bi_objs)
            )
        )

    print(f"\nRunning {len(configs)} devices. Ctrl-C to stop.")
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
