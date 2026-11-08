"""
Stage 3 acceptance check: silver tables vs data/ground_truth.jsonl.

  spark-submit /opt/spark-apps/reconcile_silver.py /tmp/ground_truth.jsonl

Comparison is per event_id, restricted to events the ground truth knows about (the
ground-truth file can miss its last buffered lines if the simulator is killed).

Definitions
  expected late      = ground-truth 'silver' event with lateness_s > WATERMARK_S
  expected silver    = ground-truth 'silver' event with lateness_s <= WATERMARK_S
  duplicates         = ground-truth 'duplicate' publishes, minus copies of events that are
                       themselves invalid (quarantined by silver, or never in bronze: dlq),
                       because silver rejects those copies as invalid rather than as dups.
  expected quarantine = ground-truth 'quarantine' + duplicate copies of quarantined events
"""
import sys

from pyspark.sql import functions as F

from silver import DQ, LATE, QUARANTINE, SILVER, WATERMARK_S, build_spark

GT_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/ground_truth.jsonl"

spark = build_spark("reconcile_silver")
spark.sparkContext.setLogLevel("ERROR")

gt = spark.read.json(GT_PATH)
print("ground-truth columns:", gt.columns)
gt = (gt.filter(F.col("expected").isNotNull() & F.col("event_id").isNotNull())
        .withColumn("lateness_s", F.coalesce(F.col("lateness_s").cast("double"), F.lit(0.0))))

ids = lambda df: df.select("event_id").distinct()
gt_all = ids(gt)
orig_silver = gt.filter(F.col("expected") == "silver")
exp_late_ids = ids(orig_silver.filter(F.col("lateness_s") > WATERMARK_S))
exp_silver_ids = ids(orig_silver.filter(F.col("lateness_s") <= WATERMARK_S))
gt_q = ids(gt.filter(F.col("expected") == "quarantine"))
gt_dlq = ids(gt.filter(F.col("expected") == "dlq"))
gt_dup = gt.filter(F.col("expected") == "duplicate").select("event_id")

silver_ids = ids(spark.table(SILVER))
late_ids = ids(spark.table(LATE))

n_exp_late = exp_late_ids.count()
n_exp_silver = exp_silver_ids.count()
n_gt_q = gt_q.count()
n_gt_dup = gt_dup.count()
dup_of_bad = gt_dup.join(gt_q, "event_id").count()
dup_of_dlq = gt_dup.join(gt_dlq, "event_id").count()

act_silver = silver_ids.join(gt_all, "event_id").count()
act_late = late_ids.join(gt_all, "event_id").count()
act_quar = spark.table(QUARANTINE).count()
act_dup = (spark.table(DQ).filter(F.col("rule").startswith("duplicate"))
           .agg(F.coalesce(F.sum("row_count"), F.lit(0))).first()[0]) or 0

lost = orig_silver.select("event_id").distinct() \
    .join(silver_ids.union(late_ids).distinct(), "event_id", "left_anti").count()
silver_total = spark.table(SILVER).count()
silver_dupe_ids = silver_total - silver_ids.count()
late_and_silver = silver_ids.join(late_ids, "event_id").count()
extra_not_in_gt = (silver_ids.union(late_ids).distinct()
                   .join(gt_all, "event_id", "left_anti").count())

# (name, expected, actual, tolerance)
checks = [
    ("silver rows (ground-truth events)", n_exp_silver, act_silver,
     max(3, int(0.002 * n_exp_silver))),
    ("late_events rows (ground-truth events)", n_exp_late, act_late,
     max(3, int(0.002 * max(n_exp_late, 1)))),
    ("quarantine rows", n_gt_q + dup_of_bad, act_quar, 3),
    ("duplicates removed", n_gt_dup - dup_of_bad - dup_of_dlq, act_dup, 3),
    ("events lost (want 0)", 0, lost, 0),
    ("duplicate ids in silver (want 0)", 0, silver_dupe_ids, 0),
    ("late events also in silver (want 0)", 0, late_and_silver, 0),
]

print(f"\n{'check':40s} {'expected':>9s} {'actual':>9s} {'delta':>7s} {'tol':>4s}  result")
failed = 0
for name, e, a, tol in checks:
    ok = abs(a - e) <= tol
    failed += not ok
    print(f"{name:40s} {e:>9} {a:>9} {a - e:>7} {tol:>4}  {'PASS' if ok else 'FAIL'}")

print(f"\nnotes: {dup_of_bad} duplicate copies of already-invalid events and {dup_of_dlq} of dlq events "
      f"are counted as invalid, not as duplicates.")
print(f"       {extra_not_in_gt} events are in the lakehouse but not in the ground truth "
      f"(ground-truth tail lost when the simulator was killed).")

if failed:
    print(f"\n{failed} check(s) failed.")
    sys.exit(1)
print("\nAll Stage 3 reconciliation checks passed.")
