"""
Stage 3 - Silver job.

bronze.raw_events  ->  silver.sensor_events   (valid, deduplicated, on time)
                       silver.quarantine      (rejected, with reason code)
                       silver.late_events     (valid but behind the watermark)
                       silver.dq_metrics      (per-batch counts, pass rate)

Per micro-batch order of operations:
  1. parse + validate         -> failures go to quarantine (reason code)
  2. dedup on event_id        -> within the batch, then against silver
  3. event-time watermark     -> events older than the watermark go to late_events
  4. write silver + metrics

Run (inside the spark container, see docs/stage3.md):
  spark-submit /opt/spark-apps/silver.py --rebuild  # wipe silver, rebuild from bronze, stop
  spark-submit /opt/spark-apps/silver.py --follow   # live: keep running, 10 s trigger
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
CATALOG = "iceberg_catalog"          # same catalog as streaming/bronze_ingest.py
BRONZE = f"{CATALOG}.bronze.raw_events"
SILVER = f"{CATALOG}.silver.sensor_events"
QUARANTINE = f"{CATALOG}.silver.quarantine"
LATE = f"{CATALOG}.silver.late_events"
DQ = f"{CATALOG}.silver.dq_metrics"
CHECKPOINT = "s3a://warehouse/checkpoints/silver_sensor_events/"

# Allowed lateness. Events older than (max event_ts seen - WATERMARK_S) are late.
WATERMARK_S = int(os.getenv("WATERMARK_SECONDS", "300"))

# Fields in the JSON payload (schemas/sensor_event.v1.json). All 11 are required.
FIELDS = ["event_id", "home_id", "room", "device_id", "sensor_type", "value",
          "unit", "event_ts", "seq", "fw_version", "produced_at"]
REQUIRED = FIELDS

# Valid range per sensor type: x-valid-ranges in the schema.
# Readings inside the range but unusual stay in silver (anomalous != invalid).
RANGES = {
    "temperature": (-10.0, 50.0),
    "humidity":    (0.0, 100.0),
    "co2":         (300.0, 5000.0),
    "power":       (0.0, 10000.0),
    "motion":      (0.0, 1.0),
    "door":        (0.0, 1.0),
}
UNITS = {"temperature": "C", "humidity": "%", "co2": "ppm",
         "power": "W", "motion": "bool", "door": "bool"}
ROOMS = ["living_room", "kitchen", "bedroom", "bathroom", "hallway"]

# Format patterns from the schema.
RE_UUID = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
RE_HOME = r"^home-[0-9]{3}$"
# NOTE: schemas/sensor_event.v1.json says [a-z]{2,4}, but the simulator's CO2 devices are
# named like "co2-0018" (digit in the prefix). Valid events must not be rejected, so the
# pattern allows digits. Fix the schema or the simulator in Stage 5.
RE_DEVICE = r"^[a-z][a-z0-9]{1,3}-[0-9]{4}$"
RE_FW = r"^[0-9]+\.[0-9]+\.[0-9]+$"
SENTINEL = -999.0



# ----------------------------------------------------------------------------
# Spark session: same catalog / MinIO settings as streaming/bronze_ingest.py
# ----------------------------------------------------------------------------
def build_spark(app_name="silver"):
    c = f"spark.sql.catalog.{CATALOG}"
    minio_user = os.environ["MINIO_WAREHOUSE_USER"]
    minio_pass = os.environ["MINIO_WAREHOUSE_PASSWORD"]
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "jdbc")
        .config(f"{c}.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config(f"{c}.jdbc.user", os.environ["POSTGRES_USER"])
        .config(f"{c}.jdbc.password", os.environ["POSTGRES_PASSWORD"])
        .config(f"{c}.warehouse", "s3://warehouse/")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", "http://minio:9000")
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.s3.access-key-id", minio_user)
        .config(f"{c}.s3.secret-access-key", minio_pass)
        .config(f"{c}.client.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )

# ----------------------------------------------------------------------------
# Tables
# ----------------------------------------------------------------------------
def create_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    event_cols = """
        event_id STRING, event_ts TIMESTAMP, produced_at TIMESTAMP,
        home_id STRING, room STRING, device_id STRING, sensor_type STRING,
        value DOUBLE, unit STRING, seq BIGINT, fw_version STRING,
        kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
        ingest_ts TIMESTAMP, silver_ts TIMESTAMP"""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {SILVER} ({event_cols})
                  USING iceberg PARTITIONED BY (days(event_ts))""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {LATE}
                  ({event_cols}, lateness_s DOUBLE, watermark_ts TIMESTAMP)
                  USING iceberg PARTITIONED BY (days(event_ts))""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {QUARANTINE} (
                  reason STRING, raw_payload STRING,
                  kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
                  kafka_ts TIMESTAMP, ingest_ts TIMESTAMP, quarantined_at TIMESTAMP)
                  USING iceberg PARTITIONED BY (days(quarantined_at))""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {DQ} (
                  batch_id BIGINT, run_ts TIMESTAMP, rule STRING,
                  action STRING, row_count BIGINT)
                  USING iceberg""")


# ----------------------------------------------------------------------------
# Parse + validate
# ----------------------------------------------------------------------------
def classify(bronze_df):
    """Adds typed columns and `reason` (NULL = row is valid)."""
    schema = StructType([StructField(f, StringType()) for f in FIELDS])
    parsed = bronze_df.select(
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_ts", "ingest_ts",
        F.col("raw_payload").cast("string").alias("raw_payload"),
        F.from_json(F.col("raw_payload").cast("string"), schema).alias("p"),
    )
    df = parsed.select("kafka_topic", "kafka_partition", "kafka_offset",
                       "kafka_ts", "ingest_ts", "raw_payload", "p.*", "p")

    # try_cast / try_to_timestamp return NULL instead of failing on bad input.
    df = (df
          .withColumn("value_d", F.expr("try_cast(value AS DOUBLE)"))
          # event_ts as ISO string; falls back to epoch seconds if numeric
          .withColumn("event_ts_t", F.coalesce(
              F.expr("try_to_timestamp(event_ts)"),
              F.expr("timestamp_seconds(try_cast(event_ts AS DOUBLE))")))
          .withColumn("produced_at_t", F.coalesce(
              F.expr("try_to_timestamp(produced_at)"),
              F.expr("timestamp_seconds(try_cast(produced_at AS DOUBLE))"))))

    all_null = F.lit(True)
    for f in FIELDS:
        all_null = all_null & F.col(f).isNull()
    missing = F.lit(False)
    for f in REQUIRED:
        missing = missing | F.col(f).isNull()

    in_range = F.lit(False)
    unit_ok = F.lit(False)
    for s, (lo, hi) in RANGES.items():
        in_range = in_range | ((F.col("sensor_type") == s) &
                               F.col("value_d").between(lo, hi))
        unit_ok = unit_ok | ((F.col("sensor_type") == s) & (F.col("unit") == UNITS[s]))

    df = df.withColumn("seq_l", F.expr("try_cast(seq AS BIGINT)"))
    bad_format = (~F.col("event_id").rlike(RE_UUID) | ~F.col("home_id").rlike(RE_HOME)
                  | ~F.col("device_id").rlike(RE_DEVICE) | ~F.col("fw_version").rlike(RE_FW))

    return df.withColumn(
        "reason",
        F.when(F.col("p").isNull() | all_null, "BAD_JSON")
         .when(missing, "MISSING_FIELD")
         .when(F.col("value_d").isNull(), "BAD_TYPE_VALUE")
         .when(F.col("event_ts_t").isNull() | F.col("produced_at_t").isNull(),
               "BAD_TYPE_TIMESTAMP")
         .when(F.col("seq_l").isNull() | (F.col("seq_l") < 0), "BAD_TYPE_SEQ")
         .when(~F.col("sensor_type").isin(list(RANGES)), "UNKNOWN_SENSOR")
         .when(F.col("value_d") == SENTINEL, "SENTINEL_VALUE")
         .when(bad_format, "BAD_FORMAT")
         .when(~F.col("room").isin(ROOMS), "INVALID_ROOM")
         .when(~unit_ok, "UNIT_MISMATCH")
         .when(~in_range, "OUT_OF_RANGE"))


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def merge_insert(spark, df, table, keys, view):
    """Idempotent insert: a retried micro-batch never writes a row twice."""
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} "
              f"WHEN NOT MATCHED THEN INSERT *")


def event_columns(df, *extra):
    return df.select(*extra,
        "event_id", F.col("event_ts_t").alias("event_ts"),
        F.col("produced_at_t").alias("produced_at"),
        "home_id", "room", "device_id", "sensor_type",
        F.col("value_d").alias("value"), "unit",
        F.col("seq_l").alias("seq"), "fw_version",
        "kafka_topic", "kafka_partition", "kafka_offset", "ingest_ts")


# ----------------------------------------------------------------------------
# One micro-batch
# ----------------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    batch_started = time.monotonic()
    spark = batch_df.sparkSession
    if batch_df.isEmpty():
        return
    now = datetime.now(timezone.utc)
    df = classify(batch_df).persist()
    n_in = df.count()

    # 1. quarantine -----------------------------------------------------------
    bad = df.filter(F.col("reason").isNotNull())
    q = bad.select("reason", "raw_payload", "kafka_topic", "kafka_partition",
                   "kafka_offset", "kafka_ts", "ingest_ts",
                   F.lit(now).alias("quarantined_at"))
    merge_insert(spark, q, QUARANTINE,
                 ["kafka_topic", "kafka_partition", "kafka_offset"], "_q")
    reason_counts = {r["reason"]: r["n"] for r in
                     bad.groupBy("reason").agg(F.count("*").alias("n")).collect()}

    # 2. dedup (a) inside the batch ----------------------------------------
    good = df.filter(F.col("reason").isNull())
    w = Window.partitionBy("event_id").orderBy("kafka_partition", "kafka_offset")
    first = (good.withColumn("_rn", F.row_number().over(w))
                 .filter("_rn = 1").drop("_rn"))
    n_good = good.count()
    n_first = first.count()
    dup_batch = n_good - n_first

    # 2. dedup (b) against silver (partition-pruned by event_ts range) -------
    dup_table = 0
    new = first
    if n_first:
        lo, hi = first.agg(F.min("event_ts_t"), F.max("event_ts_t")).first()
        in_range = lambda t: (spark.table(t)
                              .filter((F.col("event_ts") >= lo) & (F.col("event_ts") <= hi))
                              .select("event_id"))
        existing = in_range(SILVER).union(in_range(LATE)).distinct()
        new = first.join(existing, "event_id", "left_anti")
    # Cache so later counts and writes all see the same rows (silver changes below).
    new = new.persist()
    n_new = new.count()
    dup_table = n_first - n_new

    # 3. watermark (per event) -----------------------------------------------
    # front = newest event_ts that had ARRIVED before this event (running max in arrival
    # order, combined with the newest event_ts already in silver). An event is late when
    # it is more than WATERMARK_S behind that front.
    prev_max = spark.table(SILVER).agg(F.max("event_ts")).first()[0]
    n_late = n_silver = 0
    stamped = None
    if n_new:
        arrival = (Window.orderBy("kafka_ts", "kafka_partition", "kafka_offset")
                   .rowsBetween(Window.unboundedPreceding, -1))
        prev_lit = (F.lit(prev_max) if prev_max is not None
                    else F.lit(None).cast("timestamp"))
        stamped = (new
                   .withColumn("front_ts", F.greatest(prev_lit,
                                                      F.max("event_ts_t").over(arrival)))
                   .withColumn("wm_ts", F.col("front_ts") -
                               F.expr(f"INTERVAL {WATERMARK_S} SECONDS"))
                   .persist())
        is_late = F.col("front_ts").isNotNull() & (F.col("event_ts_t") < F.col("wm_ts"))
        late = stamped.filter(is_late)
        ontime = stamped.filter(~is_late)
        n_late = late.count()
        n_silver = ontime.count()

        late_out = (event_columns(late,
                                  F.col("front_ts").cast("double").alias("_front"),
                                  F.col("event_ts_t").cast("double").alias("_evt"),
                                  F.col("wm_ts").alias("watermark_ts"))
                    .withColumn("silver_ts", F.lit(now))
                    .withColumn("lateness_s", F.col("_front") - F.col("_evt")))
        late_out = late_out.select(*spark.table(LATE).columns)
        merge_insert(spark, late_out, LATE,
                     ["kafka_topic", "kafka_partition", "kafka_offset"], "_late")

        silver_out = event_columns(ontime).withColumn("silver_ts", F.lit(now))
        silver_out.writeTo(SILVER).append()

    # 4. DQ metrics ----------------------------------------------------------
    rows = [(batch_id, now, "total_in", "-", n_in),
            (batch_id, now, "silver_inserted", "accept", n_silver),
            (batch_id, now, "duplicate_in_batch", "drop (counted)", dup_batch),
            (batch_id, now, "duplicate_vs_silver", "drop (counted)", dup_table),
            (batch_id, now, "late_event", "route to late_events", n_late)]
    rows += [(batch_id, now, f"invalid:{r}", "quarantine", n)
             for r, n in reason_counts.items()]
    spark.createDataFrame(rows, "batch_id BIGINT, run_ts TIMESTAMP, rule STRING, "
                                "action STRING, row_count BIGINT").writeTo(DQ).append()

    duration_s = time.monotonic() - batch_started
    input_rate = n_in / duration_s if duration_s > 0 else 0.0
    pass_rate = n_silver / n_in if n_in else 0.0
    print(f"[silver] batch {batch_id}: in={n_in} silver={n_silver} late={n_late} "
          f"dup={dup_batch + dup_table} quarantine={sum(reason_counts.values())} "
          f"pass_rate={pass_rate:.3f} duration_s={duration_s:.3f} "
          f"input_rows_per_s={input_rate:.2f}")
    if reason_counts:
        print(f"[silver] batch {batch_id} quarantine reasons: {reason_counts}")
    if stamped is not None:
        stamped.unpersist()
    new.unpersist()
    df.unpersist()


# ----------------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------------
def replay(spark):
    """Wipe silver outputs and rebuild from bronze in arrival order.

    Bronze is sliced into 1-minute buckets of kafka_ts and fed through the same
    process_batch(), so the watermark advances like it did live. Running this
    twice must give identical counts (acceptance test 3).
    """
    for t in (SILVER, LATE, QUARANTINE, DQ):
        spark.sql(f"DELETE FROM {t}")
    bronze = spark.table(BRONZE).withColumn(
        "_bucket", F.date_trunc("minute", F.col("kafka_ts")))
    buckets = [r[0] for r in bronze.select("_bucket").distinct().orderBy("_bucket").collect()]
    for i, b in enumerate(buckets):
        process_batch(bronze.filter(F.col("_bucket") == b).drop("_bucket"), i)


def main():
    parser = argparse.ArgumentParser(
        description="Stage 3 - Silver job: process bronze events into silver tables."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--rebuild", "--replay",
        dest="rebuild",
        action="store_true",
        help="Explicit flag to wipe silver tables and rebuild from Bronze in arrival order."
    )
    group.add_argument(
        "--follow",
        action="store_true",
        help="Live mode: keep running and process new bronze data as it arrives (10 s trigger)."
    )
    args = parser.parse_args()

    spark = build_spark("silver")
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)

    if args.rebuild:
        # Explicit rebuild requested: wipe silver outputs and rebuild from bronze in arrival order.
        replay(spark)
        return

    # Live mode: keep running and process new bronze data as it arrives.
    stream = (spark.readStream.format("iceberg")
              .option("streaming-skip-overwrite-snapshots", "true")
              .option("streaming-skip-delete-snapshots", "true")
              # ~30 s of data per micro-batch so the watermark advances like it did live
              .option("streaming-max-rows-per-micro-batch", "500")
              .load(BRONZE))
    q = (stream.writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime="10 seconds")
         .start())
    q.awaitTermination()


if __name__ == "__main__":
    main()
