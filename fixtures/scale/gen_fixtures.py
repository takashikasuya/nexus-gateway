#!/usr/bin/env python3
# Copyright 2026 nexus-gateway contributors
# SPDX-License-Identifier: Apache-2.0

"""Scale fixture generator for BOS resource management verification.

Verification scenario
---------------------
The simulator (bacnet-sim-gateway, opcua-sim-gateway) runs with the FULL
set of points at all times.  Connectors are configured with the full set so
they can receive any data the simulator publishes.

The BOS (Building OS) is the system under test.  Point lists of increasing
size are imported into the BOS, which provisions the gateway accordingly.
The gateway normalises Common Events using what the BOS says — not a local
file.

    Simulator (full 2000 pts always on)
        ↕  BACnet/OPC-UA
    Connectors (full config — fixtures/scale/connectors/<id>.json)
        ↕  NATS
    Gateway  ←→  BOS  ← import fixtures/scale/<N>/point_list.csv
                          to test resource management at scale N

File layout
-----------
  fixtures/scale/
    connectors/             Full connector configs (BACNET_POINTS / OPCUA_POINTS).
      bacnet-01.json        One file per connector, always the full point set.
      bacnet-02.json        Derived from the 2000-pt master; never edited directly.
      ...
      opcua-01.json
    2000/                   Master / source of truth
      point_list.json
      point_list.csv
    1000/                   BOS import fixture — 1000-pt verification
      point_list.json
      point_list.csv
    500/                    BOS import fixture — 500-pt verification
    200/                    BOS import fixture — 200-pt verification
    gen_fixtures.py         This script

Protocol distribution (master list is interleaved 1:1)
-------------------------------------------------------
   200 pts  → BACnet 100  + OPC-UA 100
   500 pts  → BACnet 250  + OPC-UA 250
  1000 pts  → BACnet 500  + OPC-UA 500
  2000 pts  → BACnet 1000 + OPC-UA 1000

Usage
-----
  # Regenerate all scale tiers and connector files
  python3 fixtures/scale/gen_fixtures.py

  # Custom sizes
  python3 fixtures/scale/gen_fixtures.py --sizes 100 300

  # Preview distribution without writing files
  python3 fixtures/scale/gen_fixtures.py --dry-run

  # CSV only (e.g. after point metadata update)
  python3 fixtures/scale/gen_fixtures.py --csv-only
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

MASTER = Path(__file__).parent / "2000/point_list.json"
OUT_ROOT = Path(__file__).parent
DEFAULT_SIZES = [200, 500, 1000, 2000]

CSV_FIELDS = ["point_id", "connector_id", "protocol", "local_id",
              "device_ref", "unit", "writable"]


def load_master(path: Path) -> list[dict]:
    if not path.exists():
        print(f"ERROR: master list not found: {path}", file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text())


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def write_csv(path: Path, points: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for p in points:
            writer.writerow({k: p.get(k, "") for k in CSV_FIELDS})


def derive_connector_points(points: list[dict]) -> dict[str, list[dict]]:
    """Strip gateway-side fields, group by connector_id."""
    by_connector: dict[str, list[dict]] = defaultdict(list)
    for p in points:
        by_connector[p["connector_id"]].append({
            "local_id": p["local_id"],
            "device_ref": p.get("device_ref", ""),
            "unit": p.get("unit", ""),
            "writable": bool(p.get("writable", False)),
        })
    return by_connector


def generate_bos_import(size: int, master: list[dict],
                        *, csv_only: bool, dry_run: bool) -> None:
    """Generate BOS import fixtures for a given point count."""
    if size > len(master):
        print(f"  WARN: {size} > master ({len(master)}), capped")
        size = len(master)

    points = master[:size]
    proto = Counter(p["protocol"] for p in points)
    cids  = Counter(p["connector_id"] for p in points)
    print(f"  {size:>5} pts  "
          f"BACnet={proto.get('bacnet',0):>4}  OPC-UA={proto.get('opcua',0):>4}  "
          f"connectors: {dict(sorted(cids.items()))}")

    if dry_run:
        return

    out = OUT_ROOT / str(size)
    if not csv_only:
        write_json(out / "point_list.json", points)
    write_csv(out / "point_list.csv", points)


def generate_connector_configs(master: list[dict], dry_run: bool) -> None:
    """Derive full connector configs from the master list."""
    by_connector = derive_connector_points(master)
    out_dir = OUT_ROOT / "connectors"
    print(f"\nConnector configs → {out_dir}/")
    for cid, pts in sorted(by_connector.items()):
        print(f"  {cid}.json  ({len(pts)} pts)")
        if not dry_run:
            write_json(out_dir / f"{cid}.json", pts)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=DEFAULT_SIZES,
                        help=f"BOS import sizes to generate (default: {DEFAULT_SIZES})")
    parser.add_argument("--master", type=Path, default=MASTER,
                        help=f"master point list (default: {MASTER})")
    parser.add_argument("--csv-only", action="store_true",
                        help="only regenerate CSV files, skip JSON")
    parser.add_argument("--dry-run", action="store_true",
                        help="show what would be generated without writing")
    parser.add_argument("--no-connectors", action="store_true",
                        help="skip regenerating connector config files")
    args = parser.parse_args()

    master = load_master(args.master)
    proto_total = Counter(p["protocol"] for p in master)
    print(f"Master: {args.master}  total={len(master)}  {dict(proto_total)}")
    print(f"\nBOS import fixtures ({['generating','dry-run'][args.dry_run]}):")

    for size in sorted(args.sizes):
        generate_bos_import(size, master, csv_only=args.csv_only, dry_run=args.dry_run)

    if not args.no_connectors and not args.csv_only:
        generate_connector_configs(master, args.dry_run)

    if not args.dry_run:
        print("\n--- Usage ---")
        print("BOS import:  fixtures/scale/<N>/point_list.csv  (import into BOS)")
        print("Connectors:  BACNET_POINTS=$(cat fixtures/scale/connectors/bacnet-01.json)")
        print("             OPCUA_POINTS=$(cat fixtures/scale/connectors/opcua-01.json)")


if __name__ == "__main__":
    main()
