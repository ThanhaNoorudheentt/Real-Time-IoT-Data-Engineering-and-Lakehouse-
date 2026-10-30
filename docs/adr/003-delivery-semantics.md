# ADR-003: Delivery semantics and deduplication

**Status:** accepted

**Context:** MQTT QoS 1 is at-least-once; devices and the simulator can resend; Spark restarts replay from checkpoints.

**Decision:** Accept at-least-once delivery end to end and achieve *effectively exactly-once* results:
- Kafka producer in the bridge is idempotent (`acks=all`).
- Bronze is append-only and stores Kafka topic/partition/offset, so it is replayable.
- Silver deduplicates on `event_id` within an event-time watermark and writes via idempotent `MERGE`.
- Late events beyond the watermark go to a `late_events` table; invalid rows go to `quarantine` with a reason code. Nothing is silently dropped.

**Alternatives:** true exactly-once via Kafka transactions end to end (not supported by Iceberg sinks in Spark streaming in a way that survives restarts without idempotent writes anyway).

**Consequences:** Correctness is *demonstrated*, not assumed: a kill-and-restart test compares row counts and checksums against the simulator ground truth.
