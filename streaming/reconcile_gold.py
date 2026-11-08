"""
Stage 4 acceptance check: Gold aggregates, device health, and alerts vs Silver.

Run inside the spark container:
  docker compose exec spark spark-submit /opt/spark-apps/reconcile_gold.py
"""
import os
import sys

from pyspark.sql import functions as F

import gold

CATALOG = gold.CATALOG
SILVER = gold.SILVER
GOLD = gold.GOLD
ALERTS = f"{GOLD}.alerts"
CO2_THRESHOLD = float(os.getenv("CO2_THRESHOLD", "1500"))

spark = gold.build_spark()
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("              STAGE 4 GOLD & ALERTS RECONCILIATION")
print("=" * 70)

checks = []

try:
    silver = spark.table(SILVER)
    silver_count = silver.count()
    print(f"Total rows in {SILVER}: {silver_count}")

    if silver_count == 0:
        print("\nWARNING: Silver table is empty. Ingest and run silver first.")
        sys.exit(1)

    # ------------------------------------------------------------------------
    # 1. Reconcile Windowed Aggregates (1m, 5m, 1h)
    # ------------------------------------------------------------------------
    windows = {
        "home_metrics_1m": "1 minute",
        "home_metrics_5m": "5 minutes",
        "home_metrics_1h": "1 hour",
    }

    for table_name, duration in windows.items():
        gold_table = f"{GOLD}.{table_name}"
        gold_df = spark.table(gold_table)

        expected = (
            silver
            .groupBy(
                "home_id",
                "room",
                "sensor_type",
                F.window("event_ts", duration).alias("time_window"),
            )
            .agg(
                F.avg("value").alias("exp_avg"),
                F.min("value").alias("exp_min"),
                F.max("value").alias("exp_max"),
                F.count("*").alias("exp_count"),
            )
            .select(
                "home_id",
                "room",
                "sensor_type",
                F.col("time_window.start").alias("window_start"),
                F.col("time_window.end").alias("window_end"),
                "exp_avg",
                "exp_min",
                "exp_max",
                "exp_count",
            )
        )

        n_expected = expected.count()
        n_actual = gold_df.count()

        # Join expected and gold to find discrepancies
        joined = expected.join(
            gold_df,
            on=["home_id", "room", "sensor_type", "window_start"],
            how="full_outer",
        )

        discrepancies = joined.filter(
            (F.col("reading_count") != F.col("exp_count")) |
            (F.col("reading_count").isNull()) |
            (F.col("exp_count").isNull()) |
            (F.abs(F.col("avg_value") - F.col("exp_avg")) > 1e-5) |
            (F.col("min_value") != F.col("exp_min")) |
            (F.col("max_value") != F.col("exp_max"))
        ).count()

        checks.append((f"{table_name} row count", n_expected, n_actual, 0))
        checks.append((f"{table_name} discrepancies (want 0)", 0, discrepancies, 0))

    # ------------------------------------------------------------------------
    # 2. Reconcile Device Health
    # ------------------------------------------------------------------------
    health_table = f"{GOLD}.device_health"
    health_df = spark.table(health_table)

    exp_health = (
        silver
        .groupBy("home_id", "room", "device_id", "sensor_type")
        .agg(F.max("event_ts").alias("exp_last_seen"))
    )

    n_exp_devices = exp_health.count()
    n_act_devices = health_df.count()

    health_joined = exp_health.join(
        health_df,
        on=["home_id", "room", "device_id", "sensor_type"],
        how="full_outer",
    )

    health_mismatches = health_joined.filter(
        (F.col("last_seen") != F.col("exp_last_seen")) |
        (F.col("last_seen").isNull()) |
        (F.col("exp_last_seen").isNull())
    ).count()

    checks.append(("device_health device count", n_exp_devices, n_act_devices, 0))
    checks.append(("device_health last_seen mismatches", 0, health_mismatches, 0))

    # ------------------------------------------------------------------------
    # 3. Reconcile Alerts
    # ------------------------------------------------------------------------
    co2_breaches = silver.filter(
        (F.col("sensor_type") == "co2") & (F.col("value") > CO2_THRESHOLD)
    )
    n_exp_alerts = co2_breaches.select("event_id").distinct().count()

    try:
        alerts_df = spark.table(ALERTS)
        n_act_alerts = alerts_df.count()
        n_unique_alerts = alerts_df.select("alert_id").distinct().count()
        duplicate_alerts = n_act_alerts - n_unique_alerts

        # Check breach count matches
        checks.append(("CO2 threshold breaches vs alerts", n_exp_alerts, n_unique_alerts, 0))
        checks.append(("duplicate alerts in gold (want 0)", 0, duplicate_alerts, 0))

        # Check that unpublished alerts are accounted for
        unpub = alerts_df.filter(F.col("published_at").isNull()).count()
        print(f"\nAlerts status: {n_act_alerts} total, {unpub} pending publication.")
    except Exception as e:
        print(f"\nNote: Alerts table {ALERTS} not accessible or empty: {e}")
        checks.append(("CO2 threshold breaches vs alerts", n_exp_alerts, 0, 0))

    # ------------------------------------------------------------------------
    # 4. Display Results
    # ------------------------------------------------------------------------
    print(f"\n{'Check':44s} {'Expected':>9s} {'Actual':>9s} {'Delta':>7s} {'Tol':>4s}  Result")
    print("-" * 75)
    failed = 0
    for name, exp, act, tol in checks:
        diff = act - exp
        ok = abs(diff) <= tol
        failed += not ok
        status = "PASS" if ok else "FAIL"
        print(f"{name:44s} {exp:>9} {act:>9} {diff:>7} {tol:>4}  {status}")

    print("=" * 75)
    if failed:
        print(f"\nFAILED: {failed} check(s) failed.")
        sys.exit(1)
    else:
        print("\nPASSED: All Stage 4 Gold & Alerts reconciliation checks passed!")
        sys.exit(0)

finally:
    spark.stop()
