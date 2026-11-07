# Stage 3: Silver (quality and correctness)

Job: `streaming/silver.py` · Check: `streaming/reconcile_silver.py` · ADR: `docs/adr/005-silver-dedup-and-watermark.md`

## Flow

```
bronze.raw_events
   │  parse JSON, validate schema + ranges
   ├── invalid ───────────────► silver.quarantine   (reason code, raw payload kept)
   │  dedup on event_id (in batch, then against silver)
   ├── duplicate ─────────────► dropped, counted in silver.dq_metrics
   │  event-time watermark
   ├── older than watermark ──► silver.late_events  (lateness_s, watermark_ts)
   └── valid, on time ────────► silver.sensor_events (partitioned by day of event_ts)
```

## Data quality rules and actions

| Rule | Reason code | Action |
|---|---|---|
| Payload is not parseable JSON (or truncated) | `BAD_JSON` | Quarantine |
| One of the 11 required fields is missing | `MISSING_FIELD` | Quarantine |
| `value` is not numeric | `BAD_TYPE_VALUE` | Quarantine |
| `event_ts` or `produced_at` is not a timestamp | `BAD_TYPE_TIMESTAMP` | Quarantine |
| `seq` is not an integer >= 0 | `BAD_TYPE_SEQ` | Quarantine |
| `sensor_type` not in the contract | `UNKNOWN_SENSOR` | Quarantine |
| `value` equals the sentinel -999 | `SENTINEL_VALUE` | Quarantine |
| `event_id`, `home_id`, `device_id` or `fw_version` has the wrong format | `BAD_FORMAT` | Quarantine |
| `room` not in the allowed list | `INVALID_ROOM` | Quarantine |
| `unit` does not match the sensor type (e.g. temperature in `W`) | `UNIT_MISMATCH` | Quarantine |
| `value` outside `x-valid-ranges` for its sensor | `OUT_OF_RANGE` | Quarantine |
| Same `event_id` seen again | `duplicate_in_batch` / `duplicate_vs_silver` | Drop, count in `dq_metrics` |
| `event_ts` older than watermark | `late_event` | Route to `late_events` |
| Valid but unusual reading (inside range) | none | Keep in silver (anomalous is not invalid) |

Nothing is silently dropped: every bronze row ends up in exactly one of silver, quarantine, late_events,
or a counted duplicate.

**Pass rate** per batch = `silver_inserted / total_in`, stored in `silver.dq_metrics`:

```sql
SELECT batch_id,
       max(CASE WHEN rule = 'total_in' THEN row_count END)       AS total_in,
       max(CASE WHEN rule = 'silver_inserted' THEN row_count END) AS silver_rows,
       round(max(CASE WHEN rule = 'silver_inserted' THEN row_count END) * 1.0
             / max(CASE WHEN rule = 'total_in' THEN row_count END), 4) AS pass_rate
FROM silver.dq_metrics GROUP BY batch_id ORDER BY batch_id;
```

## Watermark

The watermark is evaluated **per event**: `front` is the newest `event_ts` that had already arrived
before that event (running maximum in arrival order, combined with the newest `event_ts` already in
silver). An event is late when `event_ts < front - WATERMARK_SECONDS` (default 300 s), and it is then
written to `silver.late_events` with its `lateness_s` and the `watermark_ts` it was compared with.
Out-of-order events of 1-5 s are far inside the watermark and stay in silver. The simulator delays
late events by 10-600 s, so with 300 s about half of them are late and the rest are accepted as on
time; the reconcile script applies the same threshold to the ground-truth `lateness_s`.

## Dedup state and retention

Dedup compares incoming `event_id`s with silver rows in the same `event_ts` range
(Iceberg partition pruning, so only those day partitions are read), plus a window
function inside the batch. The state is the silver table itself, so it is durable
across restarts and has no expiry. Trade-off: cost grows with the number of
partitions an event batch spans; for late or replayed data that spans many days the
check reads more partitions. If silver were pruned by retention policy, a duplicate
arriving after its original expired would be accepted as new; with the current
setup (no expiry) that cannot happen.

## Idempotency and replay

- Quarantine and late tables use `MERGE ... WHEN NOT MATCHED` on the Kafka coordinates
  (topic, partition, offset), so a retried micro-batch does not insert twice.
- Silver dedups by `event_id` before append, so a retry finds its own rows and skips them.
- By default `silver.py` wipes the silver tables and rebuilds from bronze in 1-minute arrival slices
  (`--follow` is the live streaming mode). Run it twice and compare `reconcile_silver.py` output; counts should be identical.

## How to run

The Spark container mounts `./streaming` at `/opt/spark-apps`, so `silver.py` and
`reconcile_silver.py` are placed in `streaming/` and run with `spark-submit` inside it.

```bash
cp .env.example .env                    # first time only; set POSTGRES_PASSWORD
docker compose up -d --build           # platform up
python simulator/simulator.py --sink mqtt --homes 5 --realtime --duration 0   # Ctrl-C after a few minutes

# bronze job (Stage 2) must have ingested the run
docker compose exec spark spark-submit /opt/spark-apps/bronze_ingest.py

# Stage 3
docker compose exec spark spark-submit /opt/spark-apps/silver.py
docker compose cp data/ground_truth.jsonl spark:/tmp/ground_truth.jsonl
docker compose exec spark spark-submit /opt/spark-apps/reconcile_silver.py /tmp/ground_truth.jsonl

# replay test: run the job again (it wipes silver and rebuilds from bronze), reconcile again;
# the counts must be identical
docker compose exec spark spark-submit /opt/spark-apps/silver.py
docker compose exec spark spark-submit /opt/spark-apps/reconcile_silver.py /tmp/ground_truth.jsonl

# optional live mode: keeps running and processes new bronze data as it arrives
docker compose exec spark spark-submit /opt/spark-apps/silver.py --follow
```

## Handover to Stage 4

`silver.sensor_events` (partitioned by day of `event_ts`), `silver.quarantine`, `silver.late_events`,
plus `silver.dq_metrics` for the data quality dashboard panel.

## Finding: schema vs simulator mismatch (device_id)

`schemas/sensor_event.v1.json` restricts `device_id` to `^[a-z]{2,4}-[0-9]{4}$`, but the simulator names
its CO2 devices `co2-0018` (a digit in the prefix). With the schema pattern, about 13% of valid events
(every CO2 reading) were rejected as `BAD_FORMAT`. Silver therefore uses `^[a-z][a-z0-9]{1,3}-[0-9]{4}$`.
The schema (or the simulator) should be corrected in Stage 5 so the contract and the data agree.

## Reconciliation notes

`streaming/reconcile_silver.py` compares per `event_id`, restricted to events present in the ground truth:

- A duplicate copy of an event that is itself invalid (quarantined, or sent to the DLQ by the bridge) is
  rejected as invalid, not counted as a duplicate; the check moves it to the matching expected category.
- The ground-truth file is written with buffered I/O. Killing the simulator can lose its last lines
  (about 24 records here), so a few events exist in the lakehouse that the ground truth never recorded.
  Stop the simulator with Ctrl+C to avoid this.
