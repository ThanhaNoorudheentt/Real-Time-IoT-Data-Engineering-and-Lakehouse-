#!/usr/bin/env python3
"""Run safe Iceberg compaction and explicitly requested snapshot expiry."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import re
import sys

MANAGED_TABLES = (
    "bronze.raw_events",
    "silver.sensor_events",
    "silver.late_events",
    "silver.quarantine",
    "silver.dq_metrics",
    "gold.home_metrics_1m",
    "gold.home_metrics_5m",
    "gold.home_metrics_1h",
    "gold.device_health",
    "gold.alerts",
)
TABLE_PATTERN = re.compile(r"^(bronze|silver|gold)\.[a-z][a-z0-9_]*$")


def parse_expiry_timestamp(value: str) -> str:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        timestamp = datetime.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expiry timestamp must be ISO-8601, e.g. 2025-01-01T00:00:00Z"
        ) from exc
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    timestamp = timestamp.astimezone(timezone.utc)
    if timestamp >= datetime.now(timezone.utc):
        raise argparse.ArgumentTypeError("expiry timestamp must be in the past")
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def validate_tables(values: list[str]) -> list[str]:
    if not values:
        raise ValueError("at least one Iceberg table is required")
    invalid = [table for table in values if not TABLE_PATTERN.fullmatch(table)]
    if invalid:
        raise ValueError(f"unsupported table identifier(s): {', '.join(invalid)}")
    return values


def build_statements(
    tables: list[str],
    action: str,
    expire_before: str | None = None,
    retain_last: int = 5,
) -> list[str]:
    tables = validate_tables(tables)
    if action not in {"rewrite", "expire", "all"}:
        raise ValueError("action must be rewrite, expire, or all")
    if action in {"expire", "all"} and not expire_before:
        raise ValueError("snapshot expiry requires --expire-before")
    if retain_last < 1:
        raise ValueError("retain_last must be at least one")

    statements = []
    for table in tables:
        if action in {"rewrite", "all"}:
            statements.append(
                f"CALL iceberg_catalog.system.rewrite_data_files(table => '{table}')"
            )
        if action in {"expire", "all"}:
            statements.append(
                "CALL iceberg_catalog.system.expire_snapshots("
                f"table => '{table}', "
                f"older_than => TIMESTAMP '{expire_before}', "
                f"retain_last => {retain_last})"
            )
    return statements


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=("rewrite", "expire", "all"), default="rewrite")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=list(MANAGED_TABLES),
        help="namespace.table identifiers; defaults to all managed project tables",
    )
    parser.add_argument("--expire-before", type=parse_expiry_timestamp)
    parser.add_argument("--retain-last", type=int, default=5)
    parser.add_argument(
        "--confirm-expire",
        action="store_true",
        help="required for destructive snapshot expiration",
    )
    args = parser.parse_args(argv)
    if args.action in {"expire", "all"} and not args.confirm_expire:
        parser.error("snapshot expiration is destructive; pass --confirm-expire")
    if args.action in {"expire", "all"} and not args.expire_before:
        parser.error("--expire-before is required when expiring snapshots")
    if args.retain_last < 1:
        parser.error("--retain-last must be at least one")
    try:
        args.tables = validate_tables(args.tables)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv=None):
    args = parse_args(argv)
    statements = build_statements(
        args.tables, args.action, args.expire_before, args.retain_last
    )
    from gold import build_spark

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        for statement in statements:
            print(f"[maintenance] {statement}")
            spark.sql(statement).show(truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
