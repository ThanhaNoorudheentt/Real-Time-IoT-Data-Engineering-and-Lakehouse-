import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "iceberg_catalog"
SILVER = f"{CATALOG}.silver.sensor_events"
ALERTS = f"{CATALOG}.gold.alerts"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "iot.alerts"
CO2_THRESHOLD = float(os.getenv("CO2_THRESHOLD", "1500"))


def build_spark():
    c = f"spark.sql.catalog.{CATALOG}"

    return (
        SparkSession.builder
        .appName("IoT-CO2-Alerts")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "jdbc")
        .config(f"{c}.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config(f"{c}.jdbc.user", os.environ["POSTGRES_USER"])
        .config(f"{c}.jdbc.password", os.environ["POSTGRES_PASSWORD"])
        .config(f"{c}.warehouse", "s3://warehouse/")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", "http://minio:9000")
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.s3.access-key-id", os.environ["MINIO_ROOT_USER"])
        .config(f"{c}.s3.secret-access-key", os.environ["MINIO_ROOT_PASSWORD"])
        .config(f"{c}.client.region", "us-east-1")
        .getOrCreate()
    )


def create_alert_table(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ALERTS} (
            alert_id STRING,
            event_id STRING,
            home_id STRING,
            room STRING,
            device_id STRING,
            sensor_type STRING,
            observed_value DOUBLE,
            threshold DOUBLE,
            unit STRING,
            event_time TIMESTAMP,
            detected_at TIMESTAMP,
            published_at TIMESTAMP
        )
        USING iceberg
    """)
    # Ensure published_at column exists if table was previously created without it
    if "published_at" not in spark.table(ALERTS).columns:
        spark.sql(f"ALTER TABLE {ALERTS} ADD COLUMNS (published_at TIMESTAMP)")


def publish_unpublished_alerts(spark):
    unpublished = spark.table(ALERTS).filter(F.col("published_at").isNull()).cache()
    count = unpublished.count()
    if count == 0:
        unpublished.unpersist()
        print("[alerts] No unpublished alerts.")
        return

    messages = unpublished.select(
        F.col("alert_id").alias("key"),
        F.to_json(
            F.struct(
                "alert_id", "event_id", "home_id", "room",
                "device_id", "sensor_type", "observed_value",
                "threshold", "unit", "event_time", "detected_at"
            )
        ).alias("value")
    )
    try:
        messages.select(
            F.col("key").cast("string"),
            F.col("value").cast("string")
        ).write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", KAFKA_TOPIC) \
            .save()

        published_ids = (
            unpublished.select("alert_id")
            .distinct()
            .withColumn("published_at", F.current_timestamp())
        )
        published_ids.createOrReplaceTempView("published_alert_ids")
        spark.sql(f"""
            MERGE INTO {ALERTS} AS target
            USING published_alert_ids AS source
            ON target.alert_id = source.alert_id
            WHEN MATCHED AND target.published_at IS NULL
              THEN UPDATE SET published_at = source.published_at
        """)
        print(f"[alerts] Published {count} alert(s) to {KAFKA_TOPIC}.")
    finally:
        unpublished.unpersist()


def refresh_alerts(spark):
    create_alert_table(spark)

    breaches = (
        spark.table(SILVER)
        .filter(
            (F.col("sensor_type") == "co2") &
            (F.col("value") > CO2_THRESHOLD)
        )
        .withColumn("alert_id", F.sha2(F.col("event_id"), 256))
        .withColumn("threshold", F.lit(CO2_THRESHOLD))
        .withColumnRenamed("value", "observed_value")
        .withColumnRenamed("event_ts", "event_time")
        .withColumn("detected_at", F.current_timestamp())
        .withColumn("published_at", F.lit(None).cast("timestamp"))
        .select(
            "alert_id", "event_id", "home_id", "room",
            "device_id", "sensor_type", "observed_value",
            "threshold", "unit", "event_time", "detected_at", "published_at"
        )
        .dropDuplicates(["alert_id"])
    )

    breaches.createOrReplaceTempView("new_alerts")
    spark.sql(f"""
        MERGE INTO {ALERTS} AS target
        USING new_alerts AS source
        ON target.alert_id = source.alert_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    publish_unpublished_alerts(spark)


def parse_args():
    parser = argparse.ArgumentParser(description="Create and publish CO2 alerts.")
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=0,
        help="Repeat checks at this interval; 0 runs once.",
    )
    args = parser.parse_args()
    if args.poll_interval_seconds < 0:
        parser.error("--poll-interval-seconds must be non-negative")
    return args


def main():
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        while True:
            refresh_alerts(spark)
            if args.poll_interval_seconds == 0:
                break
            time.sleep(args.poll_interval_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
