#!/usr/bin/env python3
"""Measure reproducible simulator-to-file event-generation throughput."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
SIMULATOR = ROOT / "simulator" / "simulator.py"


def run_case(homes: int, duration: int, interval: float, seed: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="iot-benchmark-") as temp_dir:
        temp = Path(temp_dir)
        truth = temp / "ground_truth.jsonl"
        messages = temp / "messages.tsv"
        command = [
            sys.executable,
            str(SIMULATOR),
            "--sink", "file",
            "--out-file", str(messages),
            "--truth-file", str(truth),
            "--homes", str(homes),
            "--duration", str(duration),
            "--interval", str(interval),
            "--seed", str(seed),
            "--start-ts", "2025-01-01T00:00:00Z",
            "--no-sleep",
            "--dup-rate", "0",
            "--late-rate", "0",
            "--ooo-rate", "0",
            "--malformed-rate", "0",
            "--dropout-prob", "0",
        ]
        started = time.perf_counter()
        result = subprocess.run(
            command, cwd=ROOT, capture_output=True, text=True, check=True
        )
        wall_seconds = time.perf_counter() - started
        summary = json.loads(result.stderr.strip().splitlines()[-1])
        published = int(summary["published"])
        return {
            "homes": homes,
            "simulated_seconds": duration,
            "interval_seconds": interval,
            "seed": seed,
            "published_events": published,
            "wall_seconds": wall_seconds,
            "events_per_second": round(published / wall_seconds, 2) if wall_seconds else 0,
            "scope": "simulator-to-local-file; excludes MQTT, Kafka, Spark, and Iceberg",
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--homes", nargs="+", type=int, default=[1, 3, 5])
    parser.add_argument("--duration", type=int, default=60, help="simulated seconds per case")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=ROOT / "docs" / "stage5-benchmarks.csv")
    args = parser.parse_args(argv)
    if any(homes < 1 for homes in args.homes):
        parser.error("each --homes value must be positive")
    if args.duration < 1 or args.interval <= 0:
        parser.error("--duration and --interval must be positive")

    rows = [
        run_case(homes, args.duration, args.interval, args.seed)
        for homes in args.homes
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps(rows, indent=2))
    print(f"Saved simulator benchmark results to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
