import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    current_timestamp
)


# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("bronze_ingest")


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "iot.raw"

CHECKPOINT_PATH = (
    "s3a://warehouse/checkpoints/bronze_raw_events/"
)

CATALOG_NAME = "iceberg_catalog"
NAMESPACE = "bronze"
TABLE_NAME = "raw_events"

POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

MINIO_USER = os.environ["MINIO_WAREHOUSE_USER"]
MINIO_PASSWORD = os.environ["MINIO_WAREHOUSE_PASSWORD"]

TABLE_IDENTIFIER = (
    f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"
)


# ---------------------------------------------------------
# Spark Session Configuration
# ---------------------------------------------------------

logger.info("Initializing Spark session...")

spark = (
    SparkSession.builder
    .appName("IoT-Bronze-Ingestion")

    # Iceberg extension
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Iceberg JDBC catalog
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.type",
        "jdbc"
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.uri",
        "jdbc:postgresql://postgres:5432/iceberg_catalog"
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.jdbc.user",
        POSTGRES_USER
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.jdbc.password",
        POSTGRES_PASSWORD
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
        "s3://warehouse/"
    )

    # Iceberg S3 FileIO configuration
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint",
        "http://minio:9000"
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access",
        "true"
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id",
        MINIO_USER
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key",
        MINIO_PASSWORD
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.client.region",
        "us-east-1"
    )

    # Hadoop S3A configuration for streaming checkpoints
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        MINIO_USER
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        MINIO_PASSWORD
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # Streaming configuration
    .config(
        "spark.sql.shuffle.partitions",
        "6"
    )
    .config(
        "spark.sql.streaming.forceDeleteTempCheckpointLocation",
        "false"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

logger.info("Spark session initialized successfully.")


# ---------------------------------------------------------
# Create Iceberg Namespace and Bronze Table
# ---------------------------------------------------------

logger.info("Creating Bronze namespace and table if required...")

spark.sql(f"""
    CREATE NAMESPACE IF NOT EXISTS
    {CATALOG_NAME}.{NAMESPACE}
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_IDENTIFIER} (

        raw_payload STRING,

        kafka_topic STRING,

        kafka_partition INT,

        kafka_offset BIGINT,

        kafka_ts TIMESTAMP,

        ingest_ts TIMESTAMP,

        mqtt_topic STRING

    )
    USING iceberg
    PARTITIONED BY (days(ingest_ts))
""")

logger.info("Bronze table is ready: %s", TABLE_IDENTIFIER)


# ---------------------------------------------------------
# Read Kafka Stream
# ---------------------------------------------------------

logger.info("Connecting to Kafka topic: %s", KAFKA_TOPIC)

kafka_stream = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )

    .option(
        "subscribe",
        KAFKA_TOPIC
    )

    .option(
        "startingOffsets",
        "earliest"
    )

    .option(
        "failOnDataLoss",
        "true"
    )

    .option(
        "includeHeaders",
        "true"
    )

    .load()
)


# ---------------------------------------------------------
# Transform Kafka Records
# ---------------------------------------------------------

bronze_stream = kafka_stream.select(

    # Original JSON payload
    col("value").cast("string").alias("raw_payload"),

    # Kafka metadata
    col("topic").alias("kafka_topic"),

    col("partition").alias("kafka_partition"),

    col("offset").alias("kafka_offset"),

    col("timestamp").alias("kafka_ts"),

    # MQTT topic extracted from Kafka headers
    expr("""
        element_at(
            transform(
                filter(
                    headers,
                    x -> x.key = 'mqtt_topic'
                ),
                x -> CAST(x.value AS STRING)
            ),
            1
        )
    """).alias("mqtt_topic"),

    # Ingestion timestamp
    current_timestamp().alias("ingest_ts")

)


# ---------------------------------------------------------
# Idempotent Batch Writer
# ---------------------------------------------------------

def write_batch(batch_df, batch_id):

    if batch_df.isEmpty():

        logger.info(
            "Batch %s is empty",
            batch_id
        )

        return

    count = batch_df.count()

    logger.info(
        "Processing batch %s containing %s records",
        batch_id,
        count
    )

    # Register temporary view in the batch DataFrame's
    # Spark session so that MERGE can access it.
    batch_df.createOrReplaceTempView("bronze_batch")

    # Merge records using Kafka topic, partition and offset.
    # This prevents duplicate records during retries/restarts.

    batch_df.sparkSession.sql(f"""

        MERGE INTO {TABLE_IDENTIFIER} AS target

        USING bronze_batch AS source

        ON target.kafka_topic = source.kafka_topic

        AND target.kafka_partition = source.kafka_partition

        AND target.kafka_offset = source.kafka_offset

        WHEN NOT MATCHED THEN INSERT (

            raw_payload,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            kafka_ts,
            ingest_ts,
            mqtt_topic

        )

        VALUES (

            source.raw_payload,
            source.kafka_topic,
            source.kafka_partition,
            source.kafka_offset,
            source.kafka_ts,
            source.ingest_ts,
            source.mqtt_topic

        )

    """)

    logger.info(
        "Batch %s committed successfully. Records processed: %s",
        batch_id,
        count
    )


# ---------------------------------------------------------
# Start Streaming Query
# ---------------------------------------------------------

logger.info("Starting Bronze streaming ingestion...")

query = (
    bronze_stream.writeStream

    .foreachBatch(write_batch)

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .trigger(
        processingTime="10 seconds"
    )

    .start()
)


# ---------------------------------------------------------
# Keep Streaming Application Running
# ---------------------------------------------------------

logger.info(
    "Bronze ingestion is running. Waiting for Kafka records..."
)

query.awaitTermination()