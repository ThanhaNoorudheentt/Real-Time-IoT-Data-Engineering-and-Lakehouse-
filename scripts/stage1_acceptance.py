#!/usr/bin/env python3
"""Stage 1 acceptance test: every message the simulator published must be accounted for in Kafka.

Runs the simulator against the live stack (MQTT -> bridge -> Kafka) and compares Kafka's
topic offsets before and after with the simulator's ground-truth log:

    messages on iot.raw  == publishes whose payload is parseable JSON
    messages on iot.dlq  == publishes whose payload is truncated/unparseable

Usage (stack must be up, `make up`):
    python3 scripts/stage1_acceptance.py [--homes 2] [--duration 300] [--seed 42]
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SIM = ROOT / "simulator" / "simulator.py"
OFFSETS_CMD = ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
               "--bootstrap-server", "kafka:9092", "--topic"]


def parse_offsets(text: str) -> int:
    """Sum the end offsets from `topic:partition:offset` lines."""
    total = 0
    for line in text.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            total += int(parts[2])
    return total


def topic_count(topic: str) -> int:
    out = subprocess.run(OFFSETS_CMD + [topic], cwd=ROOT, capture_output=True, text=True, check=True)
    return parse_offsets(out.stdout)


def expected_from_truth(truth_path: Path) -> dict:
    raw = dlq = 0
    for line in truth_path.read_text().splitlines():
        rec = json.loads(line)
        if rec.get("kind") != "publish":
            continue
        if rec["expected"] == "dlq":
            dlq += 1
        else:
            raw += 1
    return {"raw": raw, "dlq": dlq}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--homes", type=int, default=2)
    ap.add_argument("--duration", type=int, default=300, help="simulated seconds")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--settle", type=int, default=90, help="max seconds to wait for Kafka to catch up")
    ap.add_argument("--mqtt-host", default="localhost")
    args = ap.parse_args()

    truth = ROOT / "data" / "ground_truth.jsonl"
    truth.parent.mkdir(exist_ok=True)

    try:
        raw0, dlq0 = topic_count("iot.raw"), topic_count("iot.dlq")
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        sys.exit(f"FAIL: cannot read Kafka offsets. Is the stack up? (`make up`)\n{exc}")

    start = datetime.now(timezone.utc) - timedelta(seconds=args.duration)
    print(f"offsets before: iot.raw={raw0} iot.dlq={dlq0}")
    print(f"running simulator: {args.homes} homes, {args.duration}s simulated, seed {args.seed}")
    subprocess.run([sys.executable, str(SIM), "--sink", "mqtt", "--mqtt-host", args.mqtt_host,
                    "--no-sleep", "--homes", str(args.homes), "--duration", str(args.duration),
                    "--seed", str(args.seed), "--start-ts", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "--truth-file", str(truth)], check=True, cwd=ROOT)

    exp = expected_from_truth(truth)
    print(f"expected from ground truth: iot.raw +{exp['raw']}  iot.dlq +{exp['dlq']}")

    deadline, last, stable_since = time.time() + args.settle, None, time.time()
    got = {"raw": 0, "dlq": 0}
    while time.time() < deadline:
        got = {"raw": topic_count("iot.raw") - raw0, "dlq": topic_count("iot.dlq") - dlq0}
        if got == exp:
            break
        if got != last:
            last, stable_since = got, time.time()
        elif time.time() - stable_since > 15:
            break  # stopped moving and still wrong: no point waiting
        time.sleep(2)

    print(f"observed in Kafka:          iot.raw +{got['raw']}  iot.dlq +{got['dlq']}")
    if got == exp:
        print("PASS: every published message is accounted for.")
        return 0
    print(f"FAIL: raw diff {got['raw'] - exp['raw']:+d}, dlq diff {got['dlq'] - exp['dlq']:+d}")
    print("Hints: `docker compose logs bridge`, `docker compose logs mosquitto`.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
