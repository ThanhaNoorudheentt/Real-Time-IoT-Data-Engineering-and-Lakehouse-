#!/usr/bin/env python3
"""Collect Kafka-to-Bronze lag and Silver operational counts from a running stack."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess


def parse_topic_offsets(text: str, topic: str) -> dict[int, int]:
    offsets = {}
    for line in text.splitlines():
        parts = line.strip().rsplit(":", 2)
        if len(parts) != 3 or parts[0] != topic:
            continue
        try:
            partition, end_offset = int(parts[1]), int(parts[2])
        except ValueError:
            continue
        offsets[partition] = end_offset
    return offsets


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trino-host", default=os.getenv("TRINO_HOST", "localhost"))
    parser.add_argument("--trino-port", type=int, default=int(os.getenv("TRINO_PORT", "8081")))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    import trino

    connection = trino.dbapi.connect(
        host=args.trino_host,
        port=args.trino_port,
        user=os.getenv("TRINO_USER", "stage5_metrics"),
        catalog="iceberg",
        schema="bronze",
        http_scheme="http",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT kafka_partition, max(kafka_offset) "
            "FROM bronze.raw_events GROUP BY kafka_partition"
        )
        bronze_offsets = {int(part): int(offset) for part, offset in cursor.fetchall()}
        cursor.execute("SELECT count(*) FROM silver.quarantine")
        quarantine_rows = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT count(*), max(silver_ts) FROM silver.sensor_events"
        )
        silver_rows, latest_silver_ts = cursor.fetchone()
        cursor.execute(
            "SELECT max(batch_id), max(run_ts) "
            "FROM silver.dq_metrics WHERE rule = 'total_in'"
        )
        latest_batch_id, latest_batch_ts = cursor.fetchone()
    finally:
        connection.close()

    offsets_result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server", "kafka:9092", "--topic", "iot.raw",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    kafka_end_offsets = parse_topic_offsets(offsets_result.stdout, "iot.raw")
    lag_by_partition = {
        part: max(0, end_offset - (bronze_offsets.get(part, -1) + 1))
        for part, end_offset in kafka_end_offsets.items()
    }

    metrics = {
        "kafka_topic": "iot.raw",
        "kafka_end_offsets": kafka_end_offsets,
        "bronze_max_offsets": bronze_offsets,
        "bronze_offset_lag_records_by_partition": lag_by_partition,
        "bronze_offset_lag_records": sum(lag_by_partition.values()),
        "silver_rows": int(silver_rows),
        "quarantine_rows": quarantine_rows,
        "latest_silver_ts": str(latest_silver_ts) if latest_silver_ts else None,
        "latest_silver_batch_id": int(latest_batch_id) if latest_batch_id is not None else None,
        "latest_silver_batch_ts": str(latest_batch_ts) if latest_batch_ts else None,
    }
    rendered = json.dumps(metrics, indent=2, default=str)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
