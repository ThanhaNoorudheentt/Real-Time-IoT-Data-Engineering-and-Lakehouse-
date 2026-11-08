#!/usr/bin/env python3
"""Validate the project prerequisites for Stage 4 (gold, serving, and alerts)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

REQUIRED = {
    "streaming/gold.py": "Gold aggregation job",
    "streaming/alerts.py": "Alert generation job",
    "streaming/reconcile_gold.py": "Gold reconciliation check",
    "docs/stage4.md": "Stage 4 design contract",
    "monitoring/dashboards/iot-lakehouse.json": "Grafana dashboard JSON",
    "streaming/silver.py": "Silver layer source data",
}

CHECKS = [
    ("docker-compose.yml", "iot.alerts", "Kafka alert topic is configured"),
    ("docker-compose.yml", "spark:", "Spark is present for gold processing"),
    ("docker-compose.yml", "trino:", "Trino is present for serving queries"),
    ("docker-compose.yml", "grafana:", "Grafana is present for dashboards"),
    (".env.example", "POSTGRES_PASSWORD", "Postgres credential is defined"),
    (".env.example", "MINIO_WAREHOUSE_PASSWORD", "MinIO application credential is defined"),
    (".env.example", "GRAFANA_ADMIN_PASSWORD", "Grafana admin password is defined"),
]


def check_file(path: str) -> tuple[bool, str]:
    p = ROOT / path
    if not p.exists():
        return False, f"Missing required file: {path}"
    return True, ""


def main() -> int:
    failures: list[str] = []

    for rel_path, label in REQUIRED.items():
        ok, msg = check_file(rel_path)
        if not ok:
            failures.append(msg)

    for rel_path, needle, label in CHECKS:
        text = (ROOT / rel_path).read_text(encoding="utf-8")
        if needle not in text:
            failures.append(f"{label} is missing from {rel_path}")

    if failures:
        print("Stage 4 readiness check FAILED:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Stage 4 readiness check passed.")
    print("The project includes the Gold and alert jobs, dashboard, and Stage 4 prerequisites.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
