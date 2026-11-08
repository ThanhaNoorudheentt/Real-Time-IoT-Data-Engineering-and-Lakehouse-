# Smart Home IoT Lakehouse

Real-time data platform for smart home telemetry:
`devices → MQTT → Kafka → Spark Structured Streaming → Iceberg lakehouse (bronze/silver/gold) → Trino → Grafana`

## Status

The project is built in five stages, one after another. See [docs/stages.md](docs/stages.md) for tasks, handover contracts and acceptance tests.

| Stage | Goal | State |
|---|---|---|
| 1 | Foundation and ingestion ([handover](docs/stage1.md)) | complete |
| 2 | Bronze lakehouse (Postgres catalog, Spark, Iceberg, Trino) | complete |
| 3 | Silver: validation, quarantine, dedup, watermarks | complete |
| 4 | Gold, serving and alerts ([run guide](docs/stage4.md)) | complete |
| 5 | Reliability, DataOps and evaluation | not started |

## Quick start (Week 1)

```bash
cp .env.example .env          # then edit the passwords
make up                       # mosquitto, kafka, kafka-ui, minio, grafana, bridge
make ps                       # everything should be Up / Exited(0) for the *-init jobs

pip install paho-mqtt
make sim                      # live simulator -> MQTT (Ctrl-C to stop)
make consume                  # see events arriving on Kafka topic iot.raw
```

UIs: Kafka UI http://localhost:8080 · MinIO console http://localhost:9001 · Grafana http://localhost:3000

**Stage 1 done-criteria:** `make stage1` runs the unit tests, then the acceptance test, which publishes a
fixed simulated run and checks that every message is accounted for in `iot.raw` and `iot.dlq`.
You can also watch events arrive in Kafka UI.

No Docker handy? `make sim-fast` writes a reproducible run to `data/` with no broker.

## Layout

```
simulator/   fault-injecting device fleet + ground-truth log
bridge/      MQTT -> Kafka (keyed by device_id; unparseable -> iot.dlq)
schemas/     event contract (JSON Schema, with valid ranges)
streaming/   bronze / silver / gold Spark jobs        (Week 2+)
dags/        Airflow DAGs                             (Week 6)
quality/     data-quality checks                      (Week 6)
monitoring/  Prometheus / Grafana provisioning        (Week 7)
tests/       unit, integration, chaos
docs/adr/    architecture decision records
```

## Event contract

See `schemas/sensor_event.v1.json`. Dedup key = `event_id`. Event time = `event_ts` (device clock).
`produced_at` is when the device sent the message, so `produced_at - event_ts` is the device-side delay.
MQTT topic: `sensors/{home_id}/{device_id}/{sensor_type}`.

## Simulator and ground truth

Faults are seeded and reproducible. Fix `--seed` and `--start-ts` for identical runs.

| Flag | Default | Meaning |
|---|---|---|
| `--dup-rate` | 0.02 | same `event_id` published again |
| `--late-rate` | 0.05 | delayed 10-600 s, `event_ts` unchanged |
| `--ooo-rate` | 0.03 | delayed 1-5 s (arrives out of order) |
| `--malformed-rate` | 0.02 | missing field, wrong type, -999, unknown sensor, truncated JSON |
| `--dropout-prob` | 0.002 | per device per minute; silent 1-5 min |

`data/ground_truth.jsonl` has one line per publish with `faults`, `expected` destination
(`silver` / `quarantine` / `dlq` / `duplicate`) and `lateness_s`. Later, you compare lakehouse
contents against it to compute duplicate/late handling rates and completeness.

## Working as a group

- One branch per stage (`stage-2-bronze`, ...). Merge to `main` only when that stage's acceptance test passes.
- Agree table and topic contracts with the next stage's owner before building.
- Each member writes one ADR in `docs/adr/` for the main decision in their stage.
- Never commit `.env`; copy `.env.example` and edit it locally.

## Known Stage-1 shortcuts (fixed in Stage 5)

Anonymous MQTT, plaintext Kafka, default MinIO/Grafana credentials via `.env` fallbacks.
