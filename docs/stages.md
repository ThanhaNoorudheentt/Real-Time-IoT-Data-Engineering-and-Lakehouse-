# Project stages (group plan)

Five stages, built **one after another**. Each stage ends in a working, tested system that the next stage builds on. A stage is finished only when its acceptance test passes, not when its services start.

| # | Stage | Owner | Hands over | Rough time |
|---|---|---|---|---|
| 1 | Foundation and ingestion | _name_ | Events flowing into Kafka `iot.raw` | Week 1 |
| 2 | Bronze lakehouse | _name_ | `bronze.raw_events` Iceberg table, queryable in Trino | Weeks 2-3 |
| 3 | Silver: quality and correctness | _name_ | `silver.sensor_events`, `silver.quarantine`, `silver.late_events` | Weeks 3-4 |
| 4 | Gold, serving and alerts | _name_ | Gold tables, live Grafana dashboard, alerts | Weeks 5-6 |
| 5 | Reliability, DataOps and evaluation | _name_ | Test results, metrics table, final report | Weeks 6-8 |

Stage 5 starts early on one piece (the reconciliation script, below) and finishes last.

---

## Stage 1: Foundation and ingestion

**Goal:** a simulated smart home fleet publishes events through MQTT into Kafka, with a ground-truth record of what was sent.

**Tasks**
- Repo, `docker-compose.yml`, `.env`, Makefile (scaffold exists; make `make up` work on every member's laptop)
- Event schema `schemas/sensor_event.v1.json` (add `produced_at`)
- Simulator with fault injection and ground-truth log
- MQTT to Kafka bridge, topics `iot.raw`, `iot.dlq`, `iot.alerts`
- ADRs 001-004

**Hands over (contract):** Kafka topic `iot.raw` (JSON, keyed by `device_id`), `iot.dlq` for unparseable payloads, `data/ground_truth.jsonl`.

**Acceptance test**
- `make up && make sim` for 5 minutes: message count on `iot.raw` + `iot.dlq` equals the publishes in the ground-truth summary
- Same seed and start time produce identical output
- Unit tests pass in CI

---

## Stage 2: Bronze lakehouse

**Goal:** every raw event is stored, untouched, in an Iceberg table that can be queried and replayed.

**Tasks**
- Add Postgres (Iceberg JDBC catalog), Spark, Trino to compose, with memory limits
- Pin and record compatible Spark, Iceberg and Trino versions (check the release notes)
- Bronze streaming job: Kafka to Iceberg, with topic, partition, offset, `ingest_ts` and the raw payload
- Checkpoints on object storage
- **Reconciliation script** `tests/reconcile.py`: compares lakehouse tables against the ground truth (reused by every later stage)

**Hands over (contract):** table `bronze.raw_events` (`raw_payload`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_ts`, `ingest_ts`), append-only.

**Acceptance test**
- Bronze row count equals `iot.raw` message count for a fixed run
- Restart the bronze job: no gaps and no duplicate offsets
- A sample query works in Trino

---

## Stage 3: Silver: quality and correctness

**Goal:** a trusted table where each valid logical event appears once, with every rejected or late record kept and explained.

**Tasks**
- Parse and validate against the schema and valid ranges (valid-but-anomalous readings stay in silver)
- Quarantine table with a reason code
- Deduplicate on `event_id` (document state retention and what happens after it expires)
- Event-time watermark; events beyond it go to `late_events`
- Data quality rules with a pass-rate metric; document the action for each rule (reject, quarantine, flag)

**Hands over (contract):** `silver.sensor_events` (partitioned by day of `event_ts`), `silver.quarantine`, `silver.late_events`.

**Acceptance test (via the reconcile script)**
- Counts per ground-truth category match: silver, quarantine, duplicates removed
- Late events appear in `late_events`, not silently dropped
- Same input replayed from bronze produces the same silver result

---

## Stage 4: Gold, serving and alerts

**Goal:** business-level tables and a live dashboard on top of trusted data.

**Tasks**
- Windowed aggregates per home and room (1 min, 5 min, hourly: average, min, max, count)
- Device health (last seen, silent-device flag)
- Alert rules (for example CO2 above a threshold) written to `iot.alerts` and a table; each alert records event time and detection time
- Report power as average watts, not summed (summing instantaneous power is not energy)
- Grafana dashboards over Trino: live values, trends, fleet health, data quality

**Hands over (contract):** `gold.home_metrics_*`, `gold.device_health`, `gold.alerts`, dashboard JSON in `monitoring/`.

**Acceptance test**
- Gold aggregates reconcile with silver for a fixed window
- A forced threshold breach produces one alert, once, within an agreed time
- Dashboard updates while the simulator runs

---

## Stage 5: Reliability, DataOps and evaluation

**Goal:** evidence that the platform behaves correctly under failure, and the numbers for the final report.

**Tasks**
- Failure tests: kill Spark mid-run and restart, restart Kafka, restart MinIO, replay bronze into silver (baseline, failure, recovery, validation)
- Maintenance: Iceberg compaction and snapshot expiry (Airflow DAG or `make` target)
- Monitoring: Kafka lag, Spark batch duration and input rate, quarantine counts
- Security hardening: MQTT authentication and ACLs, secrets out of git, least-privilege storage keys
- CI running the integration test
- Benchmarks at several event rates; fill the metrics table
- Write-up: architecture document, data quality report, recovery report

**Hands over:** `docs/` final deliverables.

**Acceptance test**
- Recovery tests show no unexplained loss or double counting
- Metrics table is filled with measured values and the settings used
- A clean checkout runs with `make up`

---

## How the group works

- **One branch per stage**, merged only when the acceptance test passes. The next stage starts from that merge.
- **Contracts first.** The owner of stage N agrees the table and topic definitions with the owner of stage N+1 before building, so the next person can prepare against sample data (replay the simulator's output) while waiting.
- **Everyone writes one ADR** for the main decision in their stage.
- **Stage 5 owner starts early** on the reconcile script and a metrics template, so each stage is measured the same way.
- **Weekly integration check:** run the full chain as far as it exists, on one machine, and note memory use.
