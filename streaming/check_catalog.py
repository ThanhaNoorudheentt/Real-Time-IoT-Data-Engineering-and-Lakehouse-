import sys
sys.path.insert(0, "/opt/spark-apps")

import gold

spark = gold.build_spark()

try:
    print("\n--- ICEBERG NAMESPACES ---")
    spark.sql("SHOW NAMESPACES IN iceberg_catalog").show(truncate=False)

    print("\n--- SILVER TABLES ---")
    spark.sql("SHOW TABLES IN iceberg_catalog.silver").show(truncate=False)

    print("\n--- SILVER ROW COUNT ---")
    print(spark.table("iceberg_catalog.silver.sensor_events").count())

finally:
    spark.stop()