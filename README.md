# Real-Time Smart Home IoT Data Engineering & Lakehouse Platform

An enterprise-grade, end-to-end streaming data platform and lakehouse implementation for smart home IoT sensor telemetry. Built upon the **Medallion Architecture** using **MQTT**, **Apache Kafka (KRaft)**, **Apache Spark Structured Streaming**, **Apache Iceberg**, **MinIO**, **Trino**, and **Grafana**.

```
[IoT Fleet / Simulator] 
         │ (MQTT QoS 1)
         ▼
[Mosquitto MQTT Broker]
         │
         ▼
[MQTT-to-Kafka Bridge] ─── (Malformed Payloads) ───► [Kafka: iot.dlq]
         │ (Valid JSON)
         ▼
[Kafka: iot.raw] (Partitioned by device_id, 72h retention)
         │
         ▼
============================ LAKEHOUSE LAYERS ============================
[Bronze Layer]   Spark Streaming ──► Iceberg: bronze.raw_events (MinIO / S3)
                         │ (Idempotent MERGE on Kafka Coordinates)
                         ▼
[Silver Layer]   Spark Micro-Batch ──► Schema Validation, Range Checks, Quarantine
                         ├── Bad Schema / Ranges ──► Iceberg: silver.quarantine
                         ├── Older than Watermark ─► Iceberg: silver.late_events
                         ├── Duplicate event_id  ──► Dropped & Counted in silver.dq_metrics
                         └── Valid & On-Time    ──► Iceberg: silver.sensor_events
                         │
                         ▼
[Gold Layer]     Spark Serving ────► Iceberg: gold.home_metrics_1m / 5m / 1h
                         ├── Device Health     ──► Iceberg: gold.device_health
                         └── Real-Time Alerts  ──► Iceberg: gold.alerts & Kafka: iot.alerts
==========================================================================
         │
         ▼
[Serving Layer]  Trino Distributed SQL Engine (Port 8081)
         │
         ▼
[Observability]  Grafana Live Dashboards (Port 3000) & Kafka UI (Port 8080)
```

---

## Project Status & Stage Verification

The platform is designed, tested, and validated across all five developmental stages:

| Stage | Focus & Scope | Documentation & Evidence | Verification Command | State |
|---|---|---|---|---|
| **Stage 1** | Foundation & MQTT $\to$ Kafka Bridge Ingestion | [docs/stage1.md](docs/stage1.md) | `make stage1` | **Complete** |
| **Stage 2** | Bronze Lakehouse (Postgres Catalog, Iceberg, Trino) | [docs/stage2.md](docs/stage2.md) · [docs/versions.md](docs/versions.md) | `python3 tests/reconcile.py` | **Complete** |
| **Stage 3** | Silver: Quality, Quarantine, Dedup, Watermarking | [docs/stage3.md](docs/stage3.md) · [ADR-005](docs/adr/005-silver-dedup-and-watermark.md) | `docker compose exec spark spark-submit /opt/spark-apps/reconcile_silver.py /tmp/ground_truth.jsonl` | **Complete** |
| **Stage 4** | Gold: Rollups, Device Health, Outbox Alerts & Trino/Grafana | [docs/stage4.md](docs/stage4.md) | `make stage4-reconcile` | **Complete** |
| **Stage 5** | Reliability, Checkpoint Recovery, Iceberg DataOps | [docs/stage5.md](docs/stage5.md) | `python3 tests/test_restart.py` · `make test` | **Complete** |

---

## Key Architectural & Engineering Guarantees

1. **At-Least-Once Delivery $\to$ Exactly-Once Semantics ([ADR-003](docs/adr/003-delivery-semantics.md)):**
   * MQTT QoS 1 combined with an idempotent Kafka producer (`acks=all`, `enable.idempotence=True`) ensures zero edge loss.
   * Bronze records immutable Kafka coordinates `(kafka_topic, kafka_partition, kafka_offset)` and applies idempotent `MERGE INTO`.
   * Silver deduplicates on `event_id` against the live Iceberg table with partition pruning and records duplicates in `silver.dq_metrics`.
2. **Zero Silent Data Loss Guarantee:**
   * Unparseable/truncated JSON is routed by the bridge to `iot.dlq` with failure reason headers.
   * Schema/type/range violations are isolated into `silver.quarantine` with machine-readable reason codes (`BAD_JSON`, `OUT_OF_RANGE`, etc.).
   * Readings older than the dynamic watermark (`max(event_ts) - 300s`) are diverted to `silver.late_events`.
   * Valid readings enter `silver.sensor_events` partitioned by `days(event_ts)`.
3. **Physics-Correct Metric Aggregations:**
   * Power is aggregated as **average watts** across 1-minute, 5-minute, and 1-hour windows (summing instantaneous power is physically meaningless).
4. **Reliable Alert Outbox Pattern:**
   * Threshold breaches (e.g. $CO_2 > 1500\text{ ppm}$) write deterministic IDs (`sha256(event_id)`) to `gold.alerts`, publishing to Kafka topic `iot.alerts` and tracking `published_at` state.
5. **Multi-Engine ACID Lakehouse Storage ([ADR-002](docs/adr/002-lakehouse-stack.md)):**
   * Apache Iceberg on MinIO (S3 API) with a PostgreSQL JDBC catalog. Spark executes streaming/batch writes; Trino performs sub-second distributed analytical queries.

---

## Web UIs & Services

When the platform is running (`make up`), the following interfaces are available:

| Service | Port / URL | Credentials (Defaults) | Purpose |
|---|---|---|---|
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `.env` setting | Smart Home IoT monitoring dashboards |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | None | Inspect topics (`iot.raw`, `iot.dlq`, `iot.alerts`) |
| **Trino Web UI** | [http://localhost:8081](http://localhost:8081) | `trino` (no password) | Query execution stats and cluster health |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | Set via `.env` | S3 bucket inspection (`warehouse` data and checkpoints) |

---

## Quickstart & Reproduction Guide

### 1. Prerequisites
* Docker Engine 24.0+ and Docker Compose v2
* Python 3.12+
* Standard Linux utilities (`make`, `curl`)

### 2. Environment Initialization
Generate private environment configurations and start the infrastructure stack:
```bash
# Generate private .env with secure local secrets
make init-env

# Launch platform containers in background
make up

# Verify all services are healthy (or Exited(0) for bootstrap jobs)
make ps
```

### 3. Generate IoT Telemetry (Simulator)
Run the smart home simulator fleet with fault injection (duplicates, late events, malformed payloads, device dropouts):
```bash
# Real-time simulation (5 homes, Ctrl-C to stop)
make sim

# Or generate a fast 10-minute deterministic offline run for testing:
make sim-fast
```

### 4. Run Lakehouse Pipeline Layers

#### Bronze Layer (Ingestion from Kafka to Iceberg)
```bash
docker compose exec spark spark-submit /opt/spark-apps/bronze_ingest.py
```

#### Silver Layer (Validation, Deduplication, Quarantine)
```bash
# Rebuild silver tables from Bronze (requires explicit --rebuild flag):
docker compose exec spark spark-submit /opt/spark-apps/silver.py --rebuild

# Or run live streaming mode:
docker compose exec spark spark-submit /opt/spark-apps/silver.py --follow
```

#### Gold Layer & Real-Time Alerts
```bash
# Launch continuous Gold window aggregations and alert detection services:
make stage4-up

# Or execute a one-time batch calculation:
docker compose exec spark spark-submit /opt/spark-apps/gold.py
docker compose exec spark spark-submit /opt/spark-apps/alerts.py
```

---

## Automated Acceptance & Reconciliation

Every layer of the platform contains automated verification scripts reconciling against ground truth:

```bash
# 1. Run unit tests (100% CI passing)
make test

# 2. Stage 1 Acceptance: verify edge delivery and Kafka topic counts
make accept

# 3. Stage 2 Bronze Reconciliation: verify Bronze raw events against ground truth
python3 tests/reconcile.py data/ground_truth.jsonl

# 4. Stage 2 / 5 Checkpoint Restart Test: verify offset continuity & zero duplicates
python3 tests/test_restart.py

# 5. Stage 3 Silver Reconciliation: verify categories (silver, quarantine, late, dedup)
docker compose cp data/ground_truth.jsonl spark:/tmp/ground_truth.jsonl
docker compose exec spark spark-submit /opt/spark-apps/reconcile_silver.py /tmp/ground_truth.jsonl

# 6. Stage 4 Gold & Alerts Reconciliation: verify windowed rollups and alert accuracy
make stage4-reconcile
```

---

## Lakehouse Data Contract & Schema Overview

### Bronze: `iceberg_catalog.bronze.raw_events`
* **Partitioning:** `days(ingest_ts)`
* **Columns:** `raw_payload` (STRING), `kafka_topic` (STRING), `kafka_partition` (INT), `kafka_offset` (BIGINT), `kafka_ts` (TIMESTAMP), `ingest_ts` (TIMESTAMP), `mqtt_topic` (STRING).

### Silver Tables:
* **`silver.sensor_events`:** Validated, deduplicated readings partitioned by `days(event_ts)`. Contains all required fields: `event_id`, `home_id`, `room`, `device_id`, `sensor_type`, `value`, `unit`, `event_ts`, `seq`, `fw_version`, `produced_at`.
* **`silver.quarantine`:** Rejected payloads accompanied by `reason` code (`BAD_JSON`, `MISSING_FIELD`, `OUT_OF_RANGE`, `SENTINEL_VALUE`, etc.).
* **`silver.late_events`:** Events arriving beyond the watermark; tracks `lateness_s` and `watermark_ts`.
* **`silver.dq_metrics`:** Batch-level data quality metrics (`total_in`, `silver_inserted`, pass rate).

### Gold Tables:
* **`gold.home_metrics_1m` / `5m` / `1h`:** Aggregated window statistics (`avg_value`, `min_value`, `max_value`, `reading_count`) grouped by `(home_id, room, sensor_type, window_start, window_end)`.
* **`gold.device_health`:** Fleet tracking (`last_seen`, `checked_at`, `is_silent`).
* **`gold.alerts`:** Threshold breaches (`alert_id`, `event_id`, `observed_value`, `threshold`, `event_time`, `detected_at`, `published_at`).

---

## Interactive Queries in Trino

Connect via Trino CLI or your SQL client at `http://localhost:8081`:

```sql
-- 1. Check Data Quality Pass Rates across batches
SELECT batch_id,
       max(CASE WHEN rule = 'total_in' THEN row_count END) AS total_in,
       max(CASE WHEN rule = 'silver_inserted' THEN row_count END) AS silver_rows,
       round(max(CASE WHEN rule = 'silver_inserted' THEN row_count END) * 1.0 
             / max(CASE WHEN rule = 'total_in' THEN row_count END), 4) AS pass_rate
FROM iceberg.silver.dq_metrics
GROUP BY batch_id ORDER BY batch_id;

-- 2. Inspect Quarantined Records by Reason
SELECT reason, count(*) AS failure_count
FROM iceberg.silver.quarantine
GROUP BY reason ORDER BY failure_count DESC;

-- 3. Room-Level 1-Minute Temperature and CO2 Rollups
SELECT window_start, home_id, room, sensor_type, avg_value, reading_count
FROM iceberg.gold.home_metrics_1m
WHERE sensor_type IN ('temperature', 'co2')
ORDER BY window_start DESC
LIMIT 10;

-- 4. Check Generated Alerts and Detection Latency
SELECT alert_id, home_id, room, observed_value, threshold,
       event_time, detected_at,
       date_diff('second', event_time, detected_at) AS detection_lag_seconds
FROM iceberg.gold.alerts
ORDER BY detected_at DESC;
```

---

## Repository Layout

```
.
├── .github/workflows/       # GitHub Actions CI pipeline definitions
├── bridge/                  # Lightweight MQTT-to-Kafka bridge with DLQ routing
├── data/                    # Generated ground-truth and local simulation outputs
├── docker-compose.yml       # Production-aligned multi-service orchestration
├── docs/                    # Architectural specifications and stage runbooks
│   ├── adr/                 # Architecture Decision Records (ADR 001 - 005)
│   ├── stage1.md            # Stage 1: Ingestion & Edge specifications
│   ├── stage2.md            # Stage 2: Bronze Lakehouse & acceptance evidence
│   ├── stage3.md            # Stage 3: Silver quality & watermark design
│   ├── stage4.md            # Stage 4: Gold serving, alerts & metrics
│   ├── stage5.md            # Stage 5: Operations, failure recovery & evaluation
│   └── versions.md          # Pinned component versions & compatibility matrix
├── monitoring/              # Grafana dashboards & Trino datasource provisioning
├── mosquitto/               # Mosquitto broker configuration and ACL policies
├── postgres/                # PostgreSQL JDBC catalog initialization scripts
├── schemas/                 # IoT telemetry JSON Schema and range constraints
├── scripts/                 # Acceptance, readiness, and metrics collection utilities
├── simulator/               # IoT fleet simulator with seeded fault injection
├── spark/                   # Custom Spark 3.5.9 Dockerfile with Iceberg & S3 bundles
├── streaming/               # PySpark jobs (bronze_ingest, silver, gold, alerts)
├── tests/                   # Pytest unit suite, reconciliation & restart tests
└── trino/                   # Trino catalog and coordinator configuration
```

---

## Architecture Decision Records (ADRs)

* [ADR-001: Domain Selection (Smart Home IoT)](docs/adr/001-domain-smart-home.md)
* [ADR-002: Lakehouse Stack (Iceberg on MinIO, PostgreSQL JDBC Catalog, Trino)](docs/adr/002-lakehouse-stack.md)
* [ADR-003: Delivery Semantics and Deduplication Strategy](docs/adr/003-delivery-semantics.md)
* [ADR-004: S3-Compatible Object Storage Image Selection](docs/adr/004-object-storage-image.md)
* [ADR-005: Silver Deduplication State and Watermark Strategy](docs/adr/005-silver-dedup-and-watermark.md)

---

## License

This project is licensed under the [MIT License](LICENSE).
