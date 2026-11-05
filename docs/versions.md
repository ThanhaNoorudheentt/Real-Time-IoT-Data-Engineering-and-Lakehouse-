# Stage 2 - Software Versions

This document records the pinned software versions used by the Bronze Lakehouse.

## Core Components

| Component      | Version          | Runtime            | Purpose                      |
| -------------- | ---------------- | ------------------ | ---------------------------- |
| Apache Spark   | 3.5.9            | Java 11.0.31       | Distributed processing       |
| Apache Iceberg | 1.10.1           | Java 11 compatible | Lakehouse table format       |
| Scala          | 2.12             | -                  | Spark runtime compatibility  |
| Trino          | 484              | Java 25            | SQL query engine             |
| PostgreSQL     | 16               | -                  | Iceberg JDBC catalog         |
| Apache Kafka   | 3.8.0            | -                  | Event streaming              |
| MinIO          | Chainguard image | -                  | S3-compatible object storage |

## Iceberg Spark Runtime

The Spark application uses:

`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1`

Iceberg AWS bundle:

`org.apache.iceberg:iceberg-aws-bundle:1.10.1`

The runtime artifact must match the Spark and Scala versions.

## Architecture

- Kafka topic: `iot.raw`
- Iceberg catalog: JDBC
- Catalog database: PostgreSQL
- Warehouse storage: MinIO
- Bronze table: `bronze.raw_events`
- Query engine: Trino

## Compatibility References

- Apache Spark: https://spark.apache.org/
- Apache Iceberg: https://iceberg.apache.org/
- Trino: https://trino.io/

## Validation Status

- Spark 3.5.9 startup: Validated
- Java 11 compatibility with Iceberg 1.10.1 extension: Validated
- Iceberg JDBC catalog connection to PostgreSQL: Validated
- Spark and Trino namespace visibility: Validated
- Bronze streaming ingestion: Pending
- Kafka-to-Iceberg reconciliation: Pending
- Trino Bronze table queries: Pending