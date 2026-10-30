# Stage 1: Foundation and ingestion (handover)

**Status:** complete when `make stage1` passes on a clean checkout.

## What Stage 1 delivers

| Item | Where |
|---|---|
| Stack: Mosquitto, Kafka (KRaft), Kafka UI, MinIO, Grafana, bridge | `docker-compose.yml` |
| Event contract | `schemas/sensor_event.v1.json` |
| Simulator with fault injection and ground truth | `simulator/simulator.py` |
| MQTT to Kafka bridge | `bridge/bridge.py` |
| Unit tests (simulator, bridge routing) | `tests/` |
| Acceptance test against the live stack | `scripts/stage1_acceptance.py` |
| Decisions | `docs/adr/001` to `004` |

## Contract handed to Stage 2

**Kafka topics** (single broker, 6 partitions for `iot.raw`):

| Topic | Content | Key |
|---|---|---|
| `iot.raw` | Any payload that parses as a JSON object. May still violate the schema (that is Stage 3's job). | `device_id` |
| `iot.dlq` | Payloads that are not JSON objects (for example truncated). Header `reason` says why. | `device_id` |
| `iot.alerts` | Created, unused until Stage 4. | |

Each `iot.raw` record has a header `mqtt_topic`. Retention is 72 hours, so bronze can replay from the beginning within that window.

**Event fields** (all required by the schema): `event_id` (UUID, dedup key), `home_id`, `room`, `device_id`, `sensor_type`, `value`, `unit`, `event_ts` (when the reading happened), `produced_at` (when the device sent it), `seq`, `fw_version`.
`produced_at - event_ts` is the device-side delay; Kafka's record timestamp is the ingestion time.

**Ground truth:** `data/ground_truth.jsonl`, one line per publish. Fields: `event_id`, `event_ts`, `faults`, `expected` (`silver`, `quarantine`, `dlq` or `duplicate`), `lateness_s`, `publish_ts`. Dropouts appear as `kind: dropout`. This file is gitignored; regenerate it with a fixed `--seed` and `--start-ts`.

## Acceptance criteria

1. `make test` passes (16 tests).
2. `make up` brings every service up; `make ps` shows `kafka-init` and `minio-init` exited with 0.
3. `make accept` prints `PASS`: the number of messages added to `iot.raw` and `iot.dlq` equals what the ground truth says was published.
4. Same seed and start time give byte-identical simulator output (covered by a unit test).
5. Kill-the-bridge check (manual): `docker compose stop bridge`, run `make accept` in another terminal (it will wait and then report a mismatch), then `docker compose start bridge`. After a few seconds the counts should catch up, because the bridge uses a persistent MQTT session. Run `make accept` again for a clean PASS.

## Known limits (state them in the report)

- The bridge acknowledges MQTT messages when it receives them, not when Kafka confirms delivery. A crash of the bridge process between those two moments can lose a few messages. Bronze reconciliation in Stage 2 will show it if it happens.
- MQTT QoS 1 is at-least-once, so the broker can redeliver a message after a reconnect. That is a duplicate that Stage 3 must remove by `event_id`.
- Anonymous MQTT, plaintext Kafka, default credentials: local development only. Hardening is in Stage 5.
- `minio` runs from the Chainguard image because upstream MinIO removed its images (ADR-004).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `unauthorized` on `minio` pull | Use the compose file in this repo (Chainguard images). Run `docker logout` if a stale login is cached. |
| `lookup registry-1.docker.io ... i/o timeout` | DNS problem on the host: `sudo systemctl restart systemd-resolved`, or set `{"dns": ["8.8.8.8","1.1.1.1"]}` in `/etc/docker/daemon.json` and restart Docker. |
| `make accept` says it cannot read Kafka offsets | The stack is not up: `make up`, then `make ps`. |
| Counts do not match | `docker compose logs bridge` and `docker compose logs mosquitto`. If you ran the simulator twice without cleaning, this script only compares the change in offsets, so earlier runs do not matter. |
| Need a clean slate | `make clean && make up` |
