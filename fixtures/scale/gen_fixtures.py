#!/usr/bin/env python3
"""Scale fixture generator.

Derives connector point files and CSV from a point_list JSON.
Point_list is the single source of truth — connector files are generated from it.

Usage:
    # Generate specific sizes from the 2000-point master
    python3 fixtures/scale/gen_fixtures.py --sizes 200 500 1000 2000

    # Generate a custom size
    python3 fixtures/scale/gen_fixtures.py --sizes 300

    # Regenerate only CSV for an existing directory
    python3 fixtures/scale/gen_fixtures.py --sizes 2000 --csv-only

Output per size N  →  fixtures/scale/N/
    point_list.json           gateway fixture
    point_list.csv            human-readable / spreadsheet
    connectors/<id>.json      per-connector BACNET_POINTS / OPCUA_POINTS

point_list.json fields:
    point_id      globally unique identifier (used by BOS / normalizer)
    connector_id  which connector owns this point
    protocol      bacnet | opcua
    local_id      native address (e.g. "analogInput,1" or "ns=2;s=UA-F-0001")
    device_ref    device label forwarded in Common Events
    unit          engineering unit (may be empty)
    writable      whether write commands are accepted

Connector JSON fields (subset):
    local_id / device_ref / unit / writable
    — connector_id, protocol, point_id are gateway-side concerns only
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

MASTER = Path(__file__).parent / "2000/point_list.json"
OUT_ROOT = Path(__file__).parent

CSV_FIELDS = ["point_id", "connector_id", "protocol", "local_id",
              "device_ref", "unit", "writable"]


def load_master(path: Path) -> list[dict]:
    if not path.exists():
        print(f"ERROR: master list not found at {path}", file=sys.stderr)
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
    by_connector: dict[str, list[dict]] = defaultdict(list)
    for p in points:
        by_connector[p["connector_id"]].append({
            "local_id": p["local_id"],
            "device_ref": p.get("device_ref", ""),
            "unit": p.get("unit", ""),
            "writable": p.get("writable", False),
        })
    return by_connector


def generate(size: int, master: list[dict], csv_only: bool) -> None:
    if size > len(master):
        print(f"  WARN: requested {size} > master {len(master)}, using {len(master)}")
        size = len(master)

    points = master[:size]
    out_dir = OUT_ROOT / str(size)

    if not csv_only:
        write_json(out_dir / "point_list.json", points)

    write_csv(out_dir / "point_list.csv", points)

    if not csv_only:
        by_connector = derive_connector_points(points)
        for cid, cpts in sorted(by_connector.items()):
            write_json(out_dir / "connectors" / f"{cid}.json", cpts)
        connector_summary = ", ".join(
            f"{cid}({len(v)})" for cid, v in sorted(by_connector.items())
        )
    else:
        connector_summary = "(skipped)"

    proto_counts = {}
    for p in points:
        proto_counts[p["protocol"]] = proto_counts.get(p["protocol"], 0) + 1
    proto_str = " ".join(f"{k}={v}" for k, v in sorted(proto_counts.items()))

    print(f"  {size:>5} points → {out_dir}  [{proto_str}]  connectors: {connector_summary}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", type=int, nargs="+",
                        default=[200, 500, 1000, 2000],
                        help="point counts to generate (default: 200 500 1000 2000)")
    parser.add_argument("--master", type=Path, default=MASTER,
                        help=f"master point_list JSON (default: {MASTER})")
    parser.add_argument("--csv-only", action="store_true",
                        help="only (re)generate CSV, skip JSON/connector files")
    args = parser.parse_args()

    master = load_master(args.master)
    print(f"Master: {args.master} ({len(master)} points)")
    print()

    for size in sorted(args.sizes):
        generate(size, master, args.csv_only)

    print()
    print("Done. Set POINT_LIST_FILE to the desired fixtures/scale/<N>/point_list.json")


if __name__ == "__main__":
    main()
