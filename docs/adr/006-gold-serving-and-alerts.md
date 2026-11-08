# ADR 006: Batch-refresh Gold and use an Iceberg alert outbox

## Status

Accepted

## Context

Stage 4 must serve business aggregates and device health from Silver, expose them
through Trino and Grafana, and route threshold alerts to both Iceberg and Kafka.
The existing Spark deployment runs batch applications against an Iceberg JDBC
catalog and MinIO; Trino reads that same catalog.

## Decision

- Rebuild the Gold aggregate and health tables from Silver on a fixed interval.
  Each refresh is deterministic and replaces the previous table contents.
- Use average, minimum, maximum and count over event-time windows. In particular,
  instantaneous power readings remain average watts rather than being summed.
- Store each alert once in `gold.alerts`, identified by a hash of `event_id`.
  Treat rows with no `published_at` as an outbox and retry publishing them to
  Kafka. Kafka message keys use the stable `alert_id`.
- Provision Grafana's Trino plugin, datasource and dashboard as checked-in
  configuration.

## Consequences

Gold freshness is bounded by the refresh interval and the Spark job recomputes
the source aggregates on every refresh. The outbox survives Kafka outages and
avoids losing alerts when Kafka is unavailable. Iceberg and Kafka do not provide
a shared transaction; a crash between Kafka acknowledgement and updating
`published_at` can publish a duplicate. Consumers can make handling idempotent
using `alert_id`.
