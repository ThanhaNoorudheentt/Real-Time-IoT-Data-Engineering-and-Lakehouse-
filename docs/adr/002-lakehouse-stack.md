# ADR-002: Lakehouse stack: Iceberg on MinIO, JDBC catalog, Trino

**Status:** accepted

**Context:** Need open table formats with ACID writes on lake storage, locally containerized, with a query engine for dashboards.

**Decision:** Apache Iceberg on MinIO (S3 API), JDBC catalog backed by Postgres, Spark for writes, Trino for serving.

**Alternatives:** Delta Lake (tighter Spark coupling, weaker Trino write story), Hudi, Nessie catalog (nice branching, one more service).

**Consequences:**
- Hidden partitioning (`days(event_ts)`), schema evolution, snapshot expiry and compaction are available.
- Postgres doubles as Airflow metadata store later.
- Must schedule compaction: streaming writes create many small files.
