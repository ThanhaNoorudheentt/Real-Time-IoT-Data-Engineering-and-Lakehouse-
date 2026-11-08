# Stage 4: Gold, serving and alerts

Status: complete when the Gold reconciliation and alert checks pass on a clean Stage 3 handoff.

## What is delivered

- `streaming/gold.py` rebuilds `iceberg_catalog.gold.home_metrics_1m`,
  `home_metrics_5m`, `home_metrics_1h` and `device_health` from Silver.
- Each aggregate is grouped by home, room, sensor type and event-time window,
  with average, minimum, maximum and reading count. Power is represented by its
  average in watts, not a sum.
- `streaming/alerts.py` creates one stable alert ID per CO2 breach, stores alerts
  in `iceberg_catalog.gold.alerts`, and publishes unpublished alerts to Kafka
  topic `iot.alerts`. The table's `event_time` and `detected_at` distinguish
  event time from detection time.
- Grafana provisions a Trino datasource and the Smart Home IoT Lakehouse
  dashboard from `monitoring/`. The dashboard includes CO2 and power trends,
  device health, alerts and Silver pass rate.

## Run

Start the platform and finish the Stage 2/3 setup first. Silver must exist before
starting Stage 4. For a live run, keep Silver following Bronze:

```bash
docker compose up -d --build
docker compose exec spark spark-submit /opt/spark-apps/bronze_ingest.py
docker compose exec spark spark-submit /opt/spark-apps/silver.py --follow
```

In another terminal, start the Stage 4 refresh services:

```bash
make stage4-up
```

Gold refreshes every 60 seconds; alert detection polls every 10 seconds. Grafana
is available at http://localhost:3000. Open the **IoT Lakehouse** folder and
**Smart Home IoT Lakehouse** dashboard. It refreshes every 10 seconds. Stop the
jobs with `make stage4-down`; Grafana's Trino datasource uses the internal
`http://trino:8080` address.

For a one-time rebuild instead of the continuous jobs:

```bash
docker compose exec spark spark-submit /opt/spark-apps/gold.py
docker compose exec spark spark-submit /opt/spark-apps/alerts.py
```

To change the CO2 alert threshold, set `CO2_THRESHOLD` in `.env`, then recreate
the alert service with `docker compose --profile stage4 up -d --force-recreate alerts`.
The default is 1500 ppm.

## Acceptance checks

An automated reconciliation script is provided to verify all Gold aggregates, device health, and alerts against Silver:

```bash
make stage4-reconcile
# or:
docker compose exec spark spark-submit /opt/spark-apps/reconcile_gold.py
```

Manual verification checks:

1. **Gold reconciliation:** for each chosen minute, compare Silver directly with
   Gold. The following query returns matching counts and averages for the
   latest complete minute:

   ```sql
   WITH expected AS (
       SELECT home_id, room, sensor_type,
              date_trunc('minute', event_ts) AS window_start,
              avg(value) AS avg_value, min(value) AS min_value,
              max(value) AS max_value, count(*) AS reading_count
       FROM iceberg.silver.sensor_events
       WHERE event_ts < date_trunc('minute', current_timestamp)
       GROUP BY 1, 2, 3, 4
   )
   SELECT e.*, g.avg_value AS gold_avg, g.min_value AS gold_min,
          g.max_value AS gold_max, g.reading_count AS gold_count
   FROM expected e
   LEFT JOIN iceberg.gold.home_metrics_1m g
     ON g.home_id = e.home_id AND g.room = e.room
    AND g.sensor_type = e.sensor_type AND g.window_start = e.window_start
   WHERE e.window_start >= date_trunc('minute', current_timestamp) - INTERVAL '1' MINUTE
     AND (g.reading_count IS NULL OR g.reading_count <> e.reading_count
          OR abs(g.avg_value - e.avg_value) > 0.000001
          OR g.min_value <> e.min_value OR g.max_value <> e.max_value);
   ```

   It should return no rows after a Gold refresh. Repeat with `home_metrics_5m`
   and `home_metrics_1h`, adjusting the window expression accordingly.
2. **Alert idempotency:** lower `CO2_THRESHOLD` below an incoming CO2 reading,
   wait for the alert poll, and verify one row for that `event_id` in
   `iceberg.gold.alerts` and its keyed message in `iot.alerts`. Leave the alert
   job running for multiple polling intervals and verify the table still has
   one row for that alert ID.
3. **Live dashboard:** publish new simulator events while both Stage 3 and
   Stage 4 jobs are running; verify the latest Gold window and dashboard update
   within the refresh interval plus the 10-second dashboard refresh.

`gold.alerts` acts as an outbox: alert rows are deduplicated by stable `alert_id`
and unpublished rows are retried if Kafka is unavailable. A process crash after
Kafka accepts a message but before `published_at` is committed can result in a
re-publish; consumers should deduplicate by the Kafka key (`alert_id`). Kafka
and Iceberg do not share a transaction, so the cross-system delivery guarantee
is at-least-once.
