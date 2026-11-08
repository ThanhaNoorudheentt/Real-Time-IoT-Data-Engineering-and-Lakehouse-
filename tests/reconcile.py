import json
import logging
import os
import sys
from collections import Counter
from pathlib import Path

import trino


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8081"))
TRINO_USER = os.getenv("TRINO_USER", "reconciliation")

TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "bronze"
TRINO_TABLE = "raw_events"

REPORT_FILE = (
    PROJECT_ROOT / "data" / "bronze_reconciliation_report.json"
)


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("bronze_reconciliation")


# ---------------------------------------------------------
# Ground Truth Resolution and Loading
# ---------------------------------------------------------

def resolve_ground_truth_file():
    """Resolve ground truth file from CLI args, env var, or default locations."""
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        return Path(sys.argv[1])
    env_path = os.getenv("GROUND_TRUTH_FILE")
    if env_path:
        return Path(env_path)
    default_test = PROJECT_ROOT / "data" / "ground_truth_stage2_test.jsonl"
    if default_test.exists():
        return default_test
    return PROJECT_ROOT / "data" / "ground_truth.jsonl"


def load_ground_truth(file_path=None):
    gt_file = Path(file_path) if file_path else resolve_ground_truth_file()

    if not gt_file.exists():
        raise FileNotFoundError(
            f"Ground-truth file not found: {gt_file}"
        )

    expected_bronze_ids = []
    dlq_ids = []

    with open(
        gt_file,
        "r",
        encoding="utf-8"
    ) as file:

        for line_number, line in enumerate(file, start=1):

            if not line.strip():
                continue

            try:
                record = json.loads(line)

            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON at line {line_number}: {error}"
                )

            if record.get("kind") != "publish":
                continue

            event_id = record.get("event_id")

            if not event_id:
                raise ValueError(
                    f"Missing event_id at line {line_number}"
                )

            # Messages with expected == 'dlq' (e.g. truncated JSON) are sent to
            # Kafka topic 'iot.dlq' by the MQTT->Kafka bridge, not 'iot.raw'.
            # Bronze ingestion consumes 'iot.raw', so DLQ messages must not be
            # counted as missing from Bronze.
            if record.get("expected") == "dlq":
                dlq_ids.append(event_id)
                continue

            expected_bronze_ids.append(event_id)

    logger.info(
        "Loaded %d expected Bronze publish records from %s (%d routed to DLQ excluded)",
        len(expected_bronze_ids),
        gt_file.name,
        len(dlq_ids)
    )

    return expected_bronze_ids, dlq_ids


# ---------------------------------------------------------
# Fetch Bronze Event IDs Using Trino
# ---------------------------------------------------------

def fetch_bronze_event_ids():

    logger.info("Connecting to Trino...")

    connection = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http"
    )

    try:

        cursor = connection.cursor()

        query = """
            SELECT
                json_extract_scalar(
                    raw_payload,
                    '$.event_id'
                ) AS event_id

            FROM bronze.raw_events
        """

        logger.info("Executing Bronze table query...")

        cursor.execute(query)

        rows = cursor.fetchall()

        event_ids = [
            row[0]
            for row in rows
            if row[0] is not None
        ]

        logger.info(
            "Fetched %s Bronze event IDs",
            len(event_ids)
        )

        return event_ids

    finally:
        connection.close()


# ---------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------

def reconcile(ground_truth_ids, bronze_ids, dlq_ids=None):

    ground_truth_counter = Counter(ground_truth_ids)

    bronze_counter = Counter(bronze_ids)

    ground_truth_unique = set(ground_truth_ids)

    bronze_unique = set(bronze_ids)

    missing_ids = sorted(
        ground_truth_unique - bronze_unique
    )

    unexpected_ids = sorted(
        bronze_unique - ground_truth_unique
    )

    duplicate_ground_truth = {
        event_id: count
        for event_id, count in ground_truth_counter.items()
        if count > 1
    }

    duplicate_bronze = {
        event_id: count
        for event_id, count in bronze_counter.items()
        if count > 1
    }

    report = {

        "ground_truth_count": len(ground_truth_ids),

        "bronze_count": len(bronze_ids),

        "ground_truth_unique_count": len(ground_truth_unique),

        "bronze_unique_count": len(bronze_unique),

        "dlq_excluded_count": len(dlq_ids) if dlq_ids else 0,

        "missing_count": len(missing_ids),

        "missing_event_ids": missing_ids,

        "unexpected_count": len(unexpected_ids),

        "unexpected_event_ids": unexpected_ids,

        "duplicate_ground_truth_count": len(
            duplicate_ground_truth
        ),

        "duplicate_ground_truth": duplicate_ground_truth,

        "duplicate_bronze_count": len(
            duplicate_bronze
        ),

        "duplicate_bronze": duplicate_bronze,

        "status": "PASSED" if (
            len(ground_truth_ids) == len(bronze_ids)
            and ground_truth_counter == bronze_counter
        ) else "FAILED"

    }

    return report


# ---------------------------------------------------------
# Display Report
# ---------------------------------------------------------

def display_report(report):

    print("\n")
    print("=" * 60)
    print("       BRONZE LAKEHOUSE RECONCILIATION REPORT")
    print("=" * 60)

    print(
        f"Ground-truth records       : "
        f"{report['ground_truth_count']}"
    )

    if report.get("dlq_excluded_count"):
        print(
            f"DLQ records (excluded)     : "
            f"{report['dlq_excluded_count']}"
        )

    print(
        f"Bronze records             : "
        f"{report['bronze_count']}"
    )

    print(
        f"Unique ground-truth events : "
        f"{report['ground_truth_unique_count']}"
    )

    print(
        f"Unique Bronze events       : "
        f"{report['bronze_unique_count']}"
    )

    print(
        f"Missing events             : "
        f"{report['missing_count']}"
    )

    print(
        f"Unexpected events          : "
        f"{report['unexpected_count']}"
    )

    print(
        f"Duplicate Bronze events    : "
        f"{report['duplicate_bronze_count']}"
    )

    print(
        f"Duplicate ground-truth     : "
        f"{report['duplicate_ground_truth_count']}"
    )

    print("-" * 60)

    print(
        f"RECONCILIATION STATUS: {report['status']}"
    )

    print("=" * 60)


# ---------------------------------------------------------
# Save Report
# ---------------------------------------------------------

def save_report(report):

    with open(
        REPORT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            indent=4
        )

    logger.info(
        "Report saved to: %s",
        REPORT_FILE
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    try:

        ground_truth_ids, dlq_ids = load_ground_truth()

        bronze_ids = fetch_bronze_event_ids()

        report = reconcile(
            ground_truth_ids,
            bronze_ids,
            dlq_ids=dlq_ids
        )

        display_report(report)

        save_report(report)

        if report["status"] != "PASSED":
            sys.exit(1)

    except Exception:

        logger.exception(
            "Reconciliation failed"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()