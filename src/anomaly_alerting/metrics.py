"""
metrics.py
----------
Metrics Engine: computes per-period business/pipeline metrics for the
registered source table and appends them, in tidy (long) format, to the
metrics_baseline Delta table.

Every metric produced here becomes a row of:
    (table_name, period_start, metric_name, metric_value, computed_at, run_id)

Downstream, detection.py only ever needs to know a metric's *name* to look
up its history with a window function -- it never needs to know how that
metric was computed. This keeps the two engines decoupled.
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import AppConfig
from .logger import get_logger
from .utils import append_to_table, create_table_if_not_exists, get_spark, merge_upsert, read_table

logger = get_logger(__name__)

METRICS_BASELINE_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("period_start", TimestampType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_value", DoubleType(), True),
        StructField("computed_at", TimestampType(), False),
    ]
)


def ensure_metrics_baseline_table(cfg: AppConfig) -> None:
    full_name = cfg.state_table_full_name("metrics_baseline")
    create_table_if_not_exists(full_name, METRICS_BASELINE_SCHEMA)


def _period_column(df: DataFrame, timestamp_col: str, grain: str) -> DataFrame:
    """Truncates the event timestamp to the configured grain, producing the
    `period_start` bucket every metric in a run belongs to."""
    trunc_format = {"hourly": "hour", "daily": "day", "minute": "minute"}.get(grain, "hour")
    return df.withColumn("period_start", F.date_trunc(trunc_format, F.col(timestamp_col)))


def compute_metrics_for_table(cfg: AppConfig, run_id: str) -> DataFrame:
    """Reads the configured source table and computes every configured metric
    per period_start bucket, returning a tidy long-format DataFrame ready to
    append to metrics_baseline."""
    spark = get_spark()
    source_cfg = cfg.source_data
    table_name = cfg.source_table_full_name

    df = read_table(table_name)
    df = _period_column(df, source_cfg["timestamp_column"], cfg.metrics_engine["grain"])

    computed_at = datetime.now(timezone.utc)
    metric_frames: List[DataFrame] = []

    # 1. row_count per period (feeds missing/zero-activity + sudden increase/decrease)
    row_count_df = df.groupBy("period_start").agg(F.count(F.lit(1)).alias("metric_value"))
    metric_frames.append(_tag_metric(row_count_df, "row_count"))

    # 2. numeric column aggregates: sum / avg / stddev / min / max
    for col in source_cfg["numeric_metric_columns"]:
        agg_df = df.groupBy("period_start").agg(
            F.sum(F.col(col)).alias("sum"),
            F.avg(F.col(col)).alias("avg"),
            F.stddev(F.col(col)).alias("stddev"),
            F.min(F.col(col)).alias("min"),
            F.max(F.col(col)).alias("max"),
        )
        for stat in ["sum", "avg", "stddev", "min", "max"]:
            metric_frames.append(
                agg_df.select("period_start", F.col(stat).cast("double").alias("metric_value")).withColumn(
                    "metric_name", F.lit(f"{stat}_{col}")
                )
            )

    # 3. null ratio per monitored column
    total_by_period = df.groupBy("period_start").agg(F.count(F.lit(1)).alias("total"))
    for col in source_cfg["monitored_null_columns"]:
        null_count_df = df.groupBy("period_start").agg(
            F.sum(F.when(F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == ""), 1).otherwise(0)).alias(
                "null_count"
            )
        )
        ratio_df = null_count_df.join(total_by_period, "period_start").select(
            "period_start",
            (F.col("null_count") / F.col("total")).cast("double").alias("metric_value"),
        ).withColumn("metric_name", F.lit(f"null_ratio_{col}"))
        metric_frames.append(ratio_df)

    # 4. duplicate record count, based on configured duplicate key columns.
    # Duplicates of the same business key (e.g. transaction_id) can legitimately
    # land in different time buckets (a retry an hour later is still a
    # duplicate), so this is evaluated across the whole current snapshot
    # rather than per period_start bucket, and tagged to the run's latest
    # period so it still participates in period-over-period comparisons.
    dup_keys = source_cfg["duplicate_key_columns"]
    latest_period_row = df.agg(F.max("period_start").alias("latest_period")).collect()[0]
    latest_period = latest_period_row["latest_period"]

    key_counts = df.groupBy(*dup_keys).agg(F.count(F.lit(1)).alias("key_count"))
    total_dup_rows = (
        key_counts.withColumn("dup_rows", F.when(F.col("key_count") > 1, F.col("key_count") - 1).otherwise(0))
        .agg(F.sum("dup_rows").alias("total"))
        .collect()[0]["total"]
    ) or 0

    dup_df = spark.createDataFrame(
        [(latest_period, float(total_dup_rows))], ["period_start", "metric_value"]
    ).withColumn("metric_name", F.lit("duplicate_count"))
    metric_frames.append(dup_df)

    # 5. last-updated / staleness signal: max ingestion timestamp per period, as epoch seconds
    staleness_df = (
        df.groupBy("period_start")
        .agg(F.max(F.col(source_cfg["ingestion_timestamp_column"])).alias("max_ts"))
        .select("period_start", F.col("max_ts").cast("double").alias("metric_value"))
        .withColumn("metric_name", F.lit("last_updated_epoch"))
    )
    metric_frames.append(staleness_df)

    unioned = metric_frames[0]
    for frame in metric_frames[1:]:
        unioned = unioned.unionByName(frame)

    result = (
        unioned.withColumn("run_id", F.lit(run_id))
        .withColumn("table_name", F.lit(table_name))
        .withColumn("computed_at", F.lit(computed_at))
        .select("run_id", "table_name", "period_start", "metric_name", "metric_value", "computed_at")
    )
    return result


def _tag_metric(df: DataFrame, metric_name: str) -> DataFrame:
    return df.withColumn("metric_name", F.lit(metric_name))


def _write_metrics(metrics_df: DataFrame, cfg: AppConfig) -> int:
    """Writes computed metrics idempotently: one row per
    (table_name, period_start, metric_name), no matter how many times this
    run has recomputed that period's history. Re-running 01_Compute_Metrics
    reprocesses the whole source table every time (see compute_metrics_for_table),
    so a plain append would silently duplicate every historical period on
    every run -- corrupting the rolling-median baseline detection.py relies
    on (duplicate rows for one period get double-counted in the trailing
    window) and letting the same historical anomaly get re-detected and
    re-alerted on every run. An upsert keyed on the natural grain of the
    table makes re-running safe: same input, same output, no accumulation."""
    full_name = cfg.state_table_full_name("metrics_baseline")
    row_count = metrics_df.count()
    if read_table(full_name).limit(1).count() == 0:
        # First-ever write (table exists but is empty -- ensure_metrics_baseline_table
        # already guarantees it exists): a plain append is cheaper than a
        # needless empty-target merge.
        return append_to_table(metrics_df, full_name)
    merge_upsert(
        metrics_df,
        full_name,
        merge_keys=["table_name", "period_start", "metric_name"],
        update_columns=["metric_value", "run_id", "computed_at"],
    )
    logger.info("Upserted %d metric row(s) into %s", row_count, full_name)
    return row_count


def run_metrics_engine(cfg: AppConfig, registered_tables: List[str] = None) -> int:
    """Entry point used by the 01_Compute_Metrics notebook. Computes metrics for
    every registered table (currently just the one configured source table --
    the thread pool exists so this scales to many registered tables without
    code changes, matching the accelerator's parallel-scan design)."""
    ensure_metrics_baseline_table(cfg)
    run_id = str(uuid.uuid4())
    tables = registered_tables or [cfg.source_table_full_name]

    parallel_cfg = cfg.metrics_engine.get("parallelism", {})
    max_workers = parallel_cfg.get("max_workers", 1) if parallel_cfg.get("enabled") else 1

    total_rows = 0
    if max_workers <= 1 or len(tables) <= 1:
        for _ in tables:
            metrics_df = compute_metrics_for_table(cfg, run_id)
            total_rows += _write_metrics(metrics_df, cfg)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(compute_metrics_for_table, cfg, run_id): t for t in tables}
            for future in as_completed(futures):
                metrics_df = future.result()
                total_rows += _write_metrics(metrics_df, cfg)

    logger.info("Metrics engine run %s complete: %d metric row(s) written", run_id, total_rows)
    return total_rows
