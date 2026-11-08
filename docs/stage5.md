# Stage 5: Reliability, DataOps, and evaluation

Stage 5 adds repeatable operational tasks and evidence collection. Run the tests
locally first; run the recovery procedures only against a disposable or
development stack. Never use `docker compose down -v` during a recovery test.

## 1. Local secrets and access controls

For a new checkout:

```bash
make init-env
make up
```

`make init-env` creates `.env` with unique passwords, mode `0600`, and refuses
to overwrite an existing file. If you already have `.env`, run `make update-env`
to append any missing settings without changing current values; replace any
example passwords manually. The simulator and bridge
use separate MQTT identities; the ACL permits simulator publish-only and bridge
subscribe-only access. Spark and Trino use the warehouse-scoped MinIO user;
only the MinIO bootstrap container receives root credentials.

If MQTT credentials change, remove only the stopped `mosquitto-init` helper
container (`docker compose rm -f mosquitto-init`) and run `make up` so it
regenerates the password file from `.env`. The broker data volume is preserved.

## 2. Tests and stack validation

```bash
make test
docker compose config -q
```

CI runs the unit suite, validates the rendered Compose configuration, and runs
a lightweight MQTT-to-Kafka acceptance test in Docker. The Spark/Iceberg
recovery checks remain opt-in because they require the full stack and populated
lakehouse tables.

## 3. Failure/recovery exercise

Start the live data path first: `make up`, run Bronze ingestion, and keep Silver
following Bronze as described in [stage3.md](stage3.md). Publish a fixed,
ground-truthed simulator run. Save the baseline reconciliation output before
injecting a failure.

For each component, inject one failure at a time:

```bash
docker compose restart kafka
docker compose restart minio
docker compose restart spark
```

Wait for health checks and the streaming jobs to resume. Republish a separate
fixed test run after each recovery, then run:

```bash
python3 tests/test_restart.py
docker compose exec spark spark-submit /opt/spark-apps/silver.py --rebuild
docker compose exec spark spark-submit /opt/spark-apps/reconcile_silver.py /tmp/ground_truth.jsonl
make stage4-reconcile
make metrics
```

Record each failure start/end time, service logs, reconciliation counts, and
whether any duplicate Kafka coordinates or unexplained event loss occurred in
the results report. `silver.py --rebuild` is destructive to Silver outputs, but
reconstructs them from Bronze; do not run it while relying on the current Silver
tables for serving.

For a Spark process-level restart, terminate only the relevant streaming job
container, not the shared Spark service:

```bash
docker compose --profile stage4 kill -s SIGKILL gold
docker compose --profile stage4 up -d gold
```

The service restart policy should restore the job; verify it with
`make stage4-reconcile`.

## 4. Iceberg maintenance

Compaction is safe to run without expiring snapshots:

```bash
make iceberg-compact
```

Snapshot expiration is intentionally explicit and destructive. Choose a cutoff
that meets the recovery/retention policy, retain at least five recent snapshots,
and set `SNAPSHOT_EXPIRE_BEFORE` to an ISO timestamp in the past:

```bash
make iceberg-expire SNAPSHOT_EXPIRE_BEFORE=2025-01-01T00:00:00Z
```

The Spark maintenance job validates table identifiers and requires both the
cutoff and `--confirm-expire`. It targets the managed Bronze, Silver, and Gold
tables; use `--tables namespace.table ...` to narrow the list.

## 5. Monitoring and benchmarks

`make metrics` writes a JSON snapshot of Kafka end offsets, per-partition
Bronze offset lag, Silver row count/freshness, and quarantine row count.
Silver logs each batch's duration and input rows per second.

`make benchmark` runs deterministic simulator-only cases at 1, 3, and 5 homes
and writes measured rows to `docs/stage5-benchmarks.csv`. These numbers measure
simulator-to-file generation only; they are not end-to-end Kafka/Spark/Iceberg
throughput. For full-stack performance, repeat the same workloads through MQTT
and record Kafka, Silver, and Gold reconciliation results separately.

## Acceptance status / evidence

| Requirement | Evidence |
|---|---|
| Recovery without unexplained loss or duplication | Runbook above; attach outputs from `test_restart.py`, Silver and Gold reconciliation |
| Metrics gathered at multiple rates/settings | `scripts/benchmark.py`; generated `docs/stage5-benchmarks.csv` |
| Access hardening | MQTT password file + ACL, generated local credentials, MinIO warehouse-only policy |
| Repeatable maintenance | `streaming/maintenance.py`, `make iceberg-compact`, explicit snapshot-expiry target |
| Clean checkout starts | `make init-env && make up` (requires Docker and available host ports/disk) |

The recovery and full-stack throughput rows require running the stack locally;
do not report them as passed based only on the unit tests.
