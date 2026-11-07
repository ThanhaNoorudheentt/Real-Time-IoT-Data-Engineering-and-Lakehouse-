# ADR 005: Silver dedup state and watermark strategy

**Status:** accepted · **Stage:** 3 · **Date:** 2026-11-07

## Context

Silver must contain each valid logical event once, and every rejected or late record
must be kept and explained. The simulator injects duplicates (same `event_id`
re-published), out-of-order events (1-5 s) and late events (10-600 s).
Spark's built-in `dropDuplicates` with a watermark discards late data silently and
keeps its dedup state in the checkpoint, which makes it hard to route late events
to a table and hard to reproduce on replay.

## Decision

Run the silver logic in `foreachBatch` and implement the three concerns explicitly:

1. **Dedup state is the silver table itself.** Incoming `event_id`s are anti-joined
   against silver rows in the same `event_ts` range (partition pruned), after a
   window-function dedup inside the batch.
2. **Watermark is computed explicitly**: `max(event_ts) - 300 s`, where the max is
   taken over silver and the current batch. Older events are written to
   `silver.late_events` with `lateness_s` instead of being dropped.
3. **Idempotent writes** (MERGE on Kafka offsets for quarantine and late, anti-join
   for silver) so a failed and retried batch does not create duplicates.

## Consequences

- No silent loss: every row is in silver, quarantine, late_events or a counted duplicate.
- Replay from bronze is deterministic when bronze is fed in arrival order (`--replay`).
- Dedup state has no expiry, so a duplicate is detected however late it arrives,
  at the cost of reading more partitions when a batch spans many days.
- The 300 s watermark is a trade-off: a longer watermark keeps more late events in
  silver but delays the point at which data is considered final; a shorter one sends
  more events to `late_events`. It is configurable with `WATERMARK_SECONDS`.
- Slightly more code than the built-in operator, and per-batch queries on silver
  add latency; acceptable for this data volume.

## Alternatives considered

- `dropDuplicatesWithinWatermark`: bounded state, but late events are dropped
  without a trace, which fails the "late events appear in `late_events`" criterion.
- Delta/Iceberg MERGE for everything: simpler, but gives no way to count duplicates
  or separate late events.
