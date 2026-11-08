import sys
sys.path.insert(0, "/opt/spark-apps")

import gold

spark = gold.build_spark()
spark.sparkContext.setLogLevel("ERROR")

print("\n=== ICEBERG TABLES ROW COUNTS ===")
for t in [
    "iceberg_catalog.bronze.raw_events",
    "iceberg_catalog.silver.sensor_events",
    "iceberg_catalog.silver.quarantine",
    "iceberg_catalog.silver.late_events",
    "iceberg_catalog.silver.dq_metrics",
    "iceberg_catalog.gold.home_metrics_1m",
    "iceberg_catalog.gold.home_metrics_5m",
    "iceberg_catalog.gold.home_metrics_1h",
    "iceberg_catalog.gold.device_health",
    "iceberg_catalog.gold.alerts"
]:
    try:
        cnt = spark.table(t).count()
        print(f"  {t:40s}: {cnt}")
    except Exception as e:
        print(f"  {t:40s}: NOT FOUND / ERROR ({e})")

print("\n=== CHECKING S3A CHECKPOINTS ===")
sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jvm.java.net.URI("s3a://warehouse/"),
    sc._jvm.org.apache.hadoop.conf.Configuration()
)
try:
    status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path("s3a://warehouse/checkpoints/"))
    for s in status:
        print(f"  Checkpoint: {s.getPath().toString()}")
except Exception as e:
    print("  No checkpoints found or error:", e)

spark.stop()
