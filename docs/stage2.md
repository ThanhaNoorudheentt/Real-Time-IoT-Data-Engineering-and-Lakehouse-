# Stage 2: Bronze Lakehouse (Handover & Acceptance)

**Job:** `streaming/bronze_ingest.py` · **Reconciliation:** `tests/reconcile.py` · **Restart Test:** `tests/test_restart.py` · **Versions:** `docs/versions.md`

## Overview

Stage 2 establishes the lakehouse foundation on top of the Stage 1 ingestion pipeline. It consumes raw IoT messages from the Kafka topic `iot.raw` using Spark Structured Streaming and appends them untouched into an Apache Iceberg table backed by MinIO object storage and a PostgreSQL JDBC catalog. Trino is configured to query the lakehouse table directly.

## Architecture

```
                 ┌─────────────────────────────────────────────────────────┐
                 │                       STAGE 2                           │
                 │                                                         │
Kafka (iot.raw) ─┼──► Spark Structured Streaming (streaming/bronze_ingest.py)
                 │         │
                 │         ├──► Checkpoints (s3a://warehouse/checkpoints/bronze_raw_events/)
                 │         └──► Idempotent MERGE INTO
                 │                   │
                 │                   ▼
                 │         Apache Iceberg Table (bronze.raw_events)
                 │         ├── Metadata: PostgreSQL (iceberg_catalog)
                 │         └── Data Files: MinIO S3 (s3://warehouse/)
                 │                   │
                 │                   ▼
                 │         Trino Query Engine (port 8081)
                 └─────────────────────────────────────────────────────────┘
```

## Contract Handed to Stage 3

**Table:** `iceberg_catalog.bronze.raw_events`
**Partitioning:** `days(ingest_ts)`

| Column Name | Type | Description |
|---|---|---|
| `raw_payload` | `STRING` | Original unparsed JSON message payload from Kafka |
| `kafka_topic` | `STRING` | Kafka topic name (`iot.raw`) |
| `kafka_partition` | `INT` | Kafka partition index (`0..5`) |
| `kafka_offset` | `BIGINT` | Offset within the partition |
| `kafka_ts` | `TIMESTAMP` | Timestamp from the Kafka message record |
| `ingest_ts` | `TIMESTAMP` | Timestamp when the record was ingested into Bronze |
| `mqtt_topic` | `STRING` | Original MQTT topic extracted from Kafka message headers |

### Ingestion Guarantees & Idempotency

- **Append-only & Untouched:** No filtering, schema validation, or parsing is applied in Bronze. Bad or malformed payloads that are valid JSON objects remain in Bronze; Silver decides quarantine and data quality actions.
- **Idempotent Micro-Batches:** Batch writes use `MERGE INTO bronze.raw_events` matched on `(kafka_topic, kafka_partition, kafka_offset)`. Replayed batches or retries after failure insert only uncommitted rows, ensuring no duplicate Kafka offsets are committed to Bronze.
- **Persistent Checkpoints:** Streaming checkpoints are written to MinIO (`s3a://warehouse/checkpoints/bronze_raw_events/`), allowing stateful recovery across container or job restarts.

## Acceptance Criteria & Evidence

### 1. Ingestion and Message Accounting
Every message on `iot.raw` is ingested into `bronze.raw_events`.
- Payloads that are valid JSON objects are forwarded by the Stage 1 bridge to `iot.raw` and ingested into Bronze.
- Truncated/unparseable payloads are diverted to `iot.dlq` by the Stage 1 bridge. They do not enter `iot.raw` and are properly excluded during Bronze reconciliation.

### 2. Reconciliation against Ground Truth (`tests/reconcile.py`)
Run `python3 tests/reconcile.py data/ground_truth.jsonl`.
The reconciliation script:
- Compares unique event IDs and counts between simulator ground truth and Trino queries against `bronze.raw_events`.
- Excludes DLQ records (`expected == "dlq"`) which are routed to `iot.dlq` per Stage 1 design.
- Reconciles duplicated publishes (`expected == "duplicate"`) which are ingested as duplicates into Bronze.

Sample acceptance output:
```
============================================================
       BRONZE LAKEHOUSE RECONCILIATION REPORT
============================================================
Ground-truth records       : 1722
DLQ records (excluded)     : 6
Bronze records             : 1722
Unique ground-truth events : 1685
Unique Bronze events       : 1685
Missing events             : 0
Unexpected events          : 0
Duplicate Bronze events    : 37
Duplicate ground-truth     : 37
------------------------------------------------------------
RECONCILIATION STATUS: PASSED
============================================================
```

### 3. Restart and Checkpoint Recovery Test (`tests/test_restart.py`)
Run `python3 tests/test_restart.py`.
The restart test verifies that when the Bronze ingestion job is interrupted and resumed:
- **No duplicate offsets:** Every `(kafka_partition, kafka_offset)` exists at most once.
- **No offset gaps:** For each partition, all offsets between `min(offset)` and `max(offset)` form a contiguous sequence without missing values.
- Checkpoints stored in MinIO enable seamless recovery from where the job was interrupted.

Sample restart test output:
```
============================================================
      STAGE 2 BRONZE LAKEHOUSE RESTART TEST
============================================================
Partition 0: 287 offsets (0 to 286) - CONTINUOUS
Partition 1: 286 offsets (0 to 285) - CONTINUOUS
Partition 2: 288 offsets (0 to 287) - CONTINUOUS
Partition 3: 287 offsets (0 to 286) - CONTINUOUS
Partition 4: 287 offsets (0 to 286) - CONTINUOUS
Partition 5: 287 offsets (0 to 286) - CONTINUOUS
PASS: Zero duplicate offsets found in Bronze table.
PASS: All partitions are contiguous with no gaps. Checkpoint restart verified.
============================================================
RESULT: PASSED (No gaps and no duplicate offsets after restart)
```

### 4. Trino Query Verification
Trino connects to the Iceberg catalog and can query Bronze data directly:
```sql
-- Count total raw events
SELECT count(*) AS total_raw FROM iceberg.bronze.raw_events;

-- Inspect Kafka partition distribution and offset bounds
SELECT
    kafka_partition,
    min(kafka_offset) AS min_offset,
    max(kafka_offset) AS max_offset,
    count(*) AS event_count
FROM iceberg.bronze.raw_events
GROUP BY kafka_partition
ORDER BY kafka_partition;

-- Inspect sample payloads
SELECT
    kafka_partition,
    kafka_offset,
    json_extract_scalar(raw_payload, '$.device_id') AS device_id,
    json_extract_scalar(raw_payload, '$.event_ts') AS event_ts,
    json_extract_scalar(raw_payload, '$.value') AS sensor_value
FROM iceberg.bronze.raw_events
LIMIT 5;
```

## How to Run

```bash
# 1. Environment setup and launch stack
cp .env.example .env
docker compose up -d --build

# 2. Run simulator to generate traffic and ground-truth log
python3 simulator/simulator.py --sink mqtt --homes 5 --duration 300 --seed 42 --truth-file data/ground_truth.jsonl

# 3. Run Bronze streaming ingestion job
docker compose exec spark spark-submit /opt/spark-apps/bronze_ingest.py

# 4. Run Bronze reconciliation against ground truth
python3 tests/reconcile.py data/ground_truth.jsonl

# 5. Run the restart / checkpoint recovery test
python3 tests/test_restart.py
```

## Known Limits & Stage 3 Scope

- **Duplicates:** Duplicates produced by MQTT QoS 1 or simulator fault injection are preserved in Bronze. Stage 3 Silver deduplicates them by `event_id`.
- **Schema Validation:** Bronze does not inspect or validate schema compliance; invalid payloads are passed to Stage 3 Silver for quarantine.
