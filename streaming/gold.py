import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "iceberg_catalog"
SILVER = f"{CATALOG}.silver.sensor_events"
GOLD = f"{CATALOG}.gold"


def build_spark():
    c = f"spark.sql.catalog.{CATALOG}"

    return (
        SparkSession.builder
        .appName("IoT-Gold-Aggregations")
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
        .config(f"{c}.s3.access-key-id", os.environ["MINIO_WAREHOUSE_USER"])
        .config(f"{c}.s3.secret-access-key", os.environ["MINIO_WAREHOUSE_PASSWORD"])
        .config(f"{c}.client.region", "us-east-1")
        .getOrCreate()
    )


def refresh_gold(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD}")
    silver = spark.table(SILVER)

    windows = {
        "home_metrics_1m": "1 minute",
        "home_metrics_5m": "5 minutes",
        "home_metrics_1h": "1 hour",
    }

    for table_name, duration in windows.items():
        metrics = (
            silver
            .groupBy(
                "home_id",
                "room",
                "sensor_type",
                F.window("event_ts", duration).alias("time_window"),
            )
            .agg(
                F.avg("value").alias("avg_value"),
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
                F.count("*").alias("reading_count"),
            )
            .select(
                "home_id",
                "room",
                "sensor_type",
                F.col("time_window.start").alias("window_start"),
                F.col("time_window.end").alias("window_end"),
                "avg_value",
                "min_value",
                "max_value",
                "reading_count",
            )
        )

        # Power remains average watts; summing instantaneous readings is not energy.
        metrics.writeTo(f"{GOLD}.{table_name}").createOrReplace()
        print(f"[gold] Refreshed {table_name}")

    health = (
        silver
        .groupBy("home_id", "room", "device_id", "sensor_type")
        .agg(F.max("event_ts").alias("last_seen"))
        .withColumn("checked_at", F.current_timestamp())
        .withColumn(
            "is_silent",
            F.col("last_seen") < F.expr("current_timestamp() - INTERVAL 5 MINUTES"),
        )
    )
    health.writeTo(f"{GOLD}.device_health").createOrReplace()
    print("[gold] Refreshed device_health")


def parse_args():
    parser = argparse.ArgumentParser(description="Refresh Gold tables from Silver.")
    parser.add_argument(
        "--refresh-interval-seconds",
        type=int,
        default=0,
        help="Repeat refreshes at this interval; 0 runs once.",
    )
    args = parser.parse_args()
    if args.refresh_interval_seconds < 0:
        parser.error("--refresh-interval-seconds must be non-negative")
    return args


def main():
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        while True:
            refresh_gold(spark)
            if args.refresh_interval_seconds == 0:
                break
            time.sleep(args.refresh_interval_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
