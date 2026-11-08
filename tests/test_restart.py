#!/usr/bin/env python3
"""Stage 2 Bronze restart & checkpoint recovery test.

Verifies the Stage 2 acceptance criteria:
  - Restart the bronze job: no gaps and no duplicate offsets.
  - S3/MinIO checkpoints allow Spark Structured Streaming to resume exactly where it left off.
  - Queries Trino to assert zero duplicate Kafka offsets and continuous offset sequences per partition.

Can be run:
  1. Via pytest: python -m pytest -q tests/test_restart.py
     (Runs unit validation tests; skips live Trino queries if the Docker stack is not up).
  2. Standalone: python tests/test_restart.py [--host localhost] [--port 8081]
"""
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("test_restart")

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8081"))
TRINO_USER = os.getenv("TRINO_USER", "restart_test")
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "bronze"
TRINO_TABLE = "raw_events"


# ---------------------------------------------------------
# Offset continuity and duplicate verification helpers
# ---------------------------------------------------------

def check_duplicate_offsets(records: List[Tuple[int, int]]) -> List[Tuple[int, int, int]]:
    """Return a list of (partition, offset, count) for offsets with count > 1."""
    counts: Dict[Tuple[int, int], int] = {}
    for partition, offset in records:
        key = (partition, offset)
        counts[key] = counts.get(key, 0) + 1
    return [(part, off, cnt) for (part, off), cnt in counts.items() if cnt > 1]


def check_offset_continuity(offsets_by_partition: Dict[int, List[int]]) -> Dict[int, List[int]]:
    """Return a dict mapping partition -> list of missing offset numbers (gaps)."""
    gaps: Dict[int, List[int]] = {}
    for partition, offsets in offsets_by_partition.items():
        if not offsets:
            continue
        sorted_offsets = sorted(set(offsets))
        min_offset, max_offset = sorted_offsets[0], sorted_offsets[-1]
        expected_range = set(range(min_offset, max_offset + 1))
        missing = sorted(expected_range - set(sorted_offsets))
        if missing:
            gaps[partition] = missing
    return gaps


# ---------------------------------------------------------
# Unit Tests (always run in CI without needing Docker)
# ---------------------------------------------------------

def test_unit_offset_continuity_detects_gaps():
    """Verify continuity algorithm detects gaps and contiguous sequences."""
    continuous = {0: [0, 1, 2, 3, 4], 1: [10, 11, 12]}
    assert check_offset_continuity(continuous) == {}

    with_gap = {0: [0, 1, 3, 4], 1: [5, 6, 7]}
    assert check_offset_continuity(with_gap) == {0: [2]}


def test_unit_duplicate_detection():
    """Verify duplicate detection algorithm finds duplicated partition-offsets."""
    clean = [(0, 0), (0, 1), (1, 0), (1, 1)]
    assert check_duplicate_offsets(clean) == []

    dupes = [(0, 0), (0, 1), (0, 1), (1, 0), (1, 0)]
    assert sorted(check_duplicate_offsets(dupes)) == [(0, 1, 2), (1, 0, 2)]


# ---------------------------------------------------------
# Live Stack Check (Integration)
# ---------------------------------------------------------

def is_trino_accessible() -> bool:
    """Check if Trino is accessible."""
    try:
        import trino
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            http_scheme="http"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        return True
    except Exception:
        return False


def query_bronze_offsets() -> List[Tuple[int, int]]:
    """Fetch all (kafka_partition, kafka_offset) pairs from bronze.raw_events."""
    import trino
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http"
    )
    try:
        cursor = conn.cursor()
        query = f"""
            SELECT kafka_partition, kafka_offset
            FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
            ORDER BY kafka_partition, kafka_offset
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        return [(int(r[0]), int(r[1])) for r in rows if r[0] is not None and r[1] is not None]
    finally:
        conn.close()


def run_restart_verification() -> bool:
    """Run full restart verification against live Bronze table."""
    logger.info("Connecting to Trino at %s:%s to verify Bronze table...", TRINO_HOST, TRINO_PORT)
    records = query_bronze_offsets()
    if not records:
        logger.warning("No records found in bronze.raw_events table.")
        return False

    logger.info("Found %d records in bronze.raw_events", len(records))

    # Check for duplicate offsets
    dupes = check_duplicate_offsets(records)
    if dupes:
        logger.error("FAILED: Found %d duplicate partition-offset records in Bronze: %s", len(dupes), dupes[:10])
        return False
    logger.info("PASS: Zero duplicate offsets found in Bronze table.")

    # Group by partition
    offsets_by_part: Dict[int, List[int]] = {}
    for part, off in records:
        offsets_by_part.setdefault(part, []).append(off)

    gaps = check_offset_continuity(offsets_by_part)
    if gaps:
        logger.error("FAILED: Found offset gaps in partitions: %s", gaps)
        return False

    for part, offs in sorted(offsets_by_part.items()):
        logger.info("Partition %d: %d offsets (%d to %d) - CONTINUOUS", part, len(offs), min(offs), max(offs))

    logger.info("PASS: All partitions are contiguous with no gaps. Checkpoint restart verified.")
    return True


def test_live_restart_no_gaps_and_no_duplicates():
    """Pytest integration test: validates Bronze table if Trino is running."""
    if not is_trino_accessible():
        pytest.skip("Trino not reachable at {}:{} - live stack required for restart test".format(TRINO_HOST, TRINO_PORT))

    passed = run_restart_verification()
    assert passed, "Bronze lakehouse restart verification failed: gaps or duplicates detected"


def main():
    if not is_trino_accessible():
        logger.error("Cannot connect to Trino at %s:%s. Ensure the Docker stack is running (`make up`).", TRINO_HOST, TRINO_PORT)
        sys.exit(1)

    print("=" * 60)
    print("      STAGE 2 BRONZE LAKEHOUSE RESTART TEST")
    print("=" * 60)
    success = run_restart_verification()
    print("=" * 60)
    if success:
        print("RESULT: PASSED (No gaps and no duplicate offsets after restart)")
        sys.exit(0)
    else:
        print("RESULT: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
