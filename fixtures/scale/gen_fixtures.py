#!/usr/bin/env python3
"""Scale fixture generator.

Point list (2000/point_list.json) is the single source of truth.
Connector files are DERIVED from it — never edit them directly.

The master list is interleaved 1:1 (BACnet, OPC-UA, BACnet, OPC-UA …)
so any N-point slice always contains a proportional mix of both protocols.

Usage:
    # Standard scales
    python3 fixtures/scale/gen_fixtures.py

    # Custom sizes
    python3 fixtures/scale/gen_fixtures.py --sizes 100 300 800

    # CSV only (no JSON/connector update)
    python3 fixtures/scale/gen_fixtures.py --csv-only

    # Show protocol distribution without generating files
    python3 fixtures/scale/gen_fixtures.py --dry-run

Scale tiers and expected protocol distribution:
    200  →  100 BACnet (bacnet-01 partial)  + 100 OPC-UA
    500  →  250 BACnet (bacnet-01..02 + partial)  + 250 OPC-UA
   1000  →  500 BACnet (bacnet-01..03 + partials) + 500 OPC-UA
   2000  →  1000 BACnet (bacnet-01..05 full)      + 1000 OPC-UA

BACnet scaling model (analogous to multi-device BACnet):
    Each connector (bacnet-01..05) owns up to 200 points for one physical device.
    As point count grows, new connectors (= new BACnet devices) come online.

OPC-UA scaling model:
    Single connector (opcua-01) subscribes to all OPC-UA nodes on one server.
    One OPC-UA server can host thousands of nodes, so no new connectors needed
    until a second OPC-UA server is introduced.
    To add a second OPC-UA server: add opcua-02 entries to the master list with
    connector_id="opcua-02" and rerun this script.

point_list.json fields:
    point_id      globally unique identifier (used by BOS / normalizer)
    connector_id  which connector owns this point
    protocol      bacnet | opcua
    local_id      native address ("analogInput,1" or "ns=2;s=UA-F-0001")
    device_ref    device label forwarded in Common Events
    unit          engineering unit (may be empty)
    writable      whether write commands are accepted

Connector JSON (subset — no point_id / protocol / connector_id):
    local_id / device_ref / unit / writable
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
    """Group points by connector_id, stripping gateway-side fields."""
    by_connector: dict[str, list[dict]] = defaultdict(list)
    for p in points:
        by_connector[p["connector_id"]].append({
            "local_id": p["local_id"],
            "device_ref": p.get("device_ref", ""),
            "unit": p.get("unit", ""),
            "writable": bool(p.get("writable", False)),
        })
    return by_connector


def generate(size: int, master: list[dict], *, csv_only: bool, dry_run: bool) -> None:
    if size > len(master):
        print(f"  WARN: {size} > master ({len(master)}), capped")
        size = len(master)

    points = master[:size]
    proto_counts = Counter(p["protocol"] for p in points)
    proto_str = "  ".join(f"{k}={v}" for k, v in sorted(proto_counts.items()))

    by_connector = derive_connector_points(points)
    connector_summary = "  ".join(
        f"{cid}({len(v)})" for cid, v in sorted(by_connector.items())
    )

    print(f"  {size:>5} pts  [{proto_str}]  connectors: {connector_summary}")

    if dry_run:
        return

    out_dir = OUT_ROOT / str(size)

    if not csv_only:
        write_json(out_dir / "point_list.json", points)
        for cid, cpts in by_connector.items():
            write_json(out_dir / "connectors" / f"{cid}.json", cpts)

    write_csv(out_dir / "point_list.csv", points)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=DEFAULT_SIZES,
                        help=f"point counts to generate (default: {DEFAULT_SIZES})")
    parser.add_argument("--master", type=Path, default=MASTER,
                        help=f"master point list JSON (default: {MASTER})")
    parser.add_argument("--csv-only", action="store_true",
                        help="only (re)generate CSV, skip JSON and connector files")
    parser.add_argument("--dry-run", action="store_true",
                        help="show distribution without writing files")
    args = parser.parse_args()

    master = load_master(args.master)
    proto_total = Counter(p["protocol"] for p in master)
    print(f"Master: {args.master.name}  total={len(master)}  {dict(proto_total)}")
    print()

    for size in sorted(args.sizes):
        generate(size, master, csv_only=args.csv_only, dry_run=args.dry_run)

    if not args.dry_run:
        print()
        print("Gateway env:  POINT_LIST_FILE=/fixtures/scale/<N>/point_list.json")
        print("Regen CSV:    python3 fixtures/scale/gen_fixtures.py --csv-only")


if __name__ == "__main__":
    main()
