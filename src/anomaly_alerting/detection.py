"""
detection.py
------------
Anomaly Detection Engine: evaluates the seven configured anomaly types
against metrics_baseline (and, for row-level value anomalies, the source
table itself), writing every detection to anomaly_detection_log.

Each `_detect_*` function is independent and returns a DataFrame matching
ANOMALY_LOG_SCHEMA. run_detection_engine() unions whichever of the seven are
enabled in config.json and appends the result.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import AppConfig
from .logger import get_logger
from .utils import append_to_table, create_table_if_not_exists, get_spark, read_table

logger = get_logger(__name__)

ANOMALY_LOG_SCHEMA = StructType(
    [
        StructField("anomaly_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("anomaly_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("period_start", TimestampType(), True),
        StructField("metric_name", StringType(), True),
        StructField("detected_value", DoubleType(), True),
        StructField("baseline_value", DoubleType(), True),
        StructField("threshold_used", StringType(), True),
        StructField("message", StringType(), False),
        StructField("detected_at", TimestampType(), False),
        StructField("alerted", BooleanType(), False),
    ]
)


def ensure_anomaly_log_table(cfg: AppConfig) -> None:
    create_table_if_not_exists(cfg.state_table_full_name("anomaly_detection_log"), ANOMALY_LOG_SCHEMA)


def _empty_log(spark) -> DataFrame:
    return spark.createDataFrame([], ANOMALY_LOG_SCHEMA)


def _base_row(
    spark,
    table_name: str,
    run_id: str,
    anomaly_type: str,
    severity: str,
    column_name: str = None,
) -> Dict:
    return dict(
        run_id=run_id,
        table_name=table_name,
        column_name=column_name,
        anomaly_type=anomaly_type,
        severity=severity,
        detected_at=datetime.now(timezone.utc),
        alerted=False,
    )


def _metric_history(cfg: AppConfig, metric_name: str) -> DataFrame:
    """Returns metrics_baseline rows for one metric, with a `previous_value`
    column populated via a lag window function ordered by period_start.
    Kept for reference/debugging; anomaly checks use
    _metric_history_with_rolling_baseline() below instead of this alone."""
    baseline = read_table(cfg.state_table_full_name("metrics_baseline"))
    filtered = baseline.filter(F.col("metric_name") == metric_name)
    window = Window.partitionBy("table_name").orderBy("period_start")
    return filtered.withColumn("previous_value", F.lag("metric_value").over(window))


def _metric_history_with_rolling_baseline(cfg: AppConfig, metric_name: str) -> DataFrame:
    """Returns metrics_baseline rows for one metric with a rolling baseline
    computed over the trailing `metrics_engine.baseline_window_periods`
    periods, excluding the current period -- the moving-baseline / n-sigma
    ("control chart") pattern production monitoring tools (Datadog, AWS
    CloudWatch, Prometheus-based alerting) use instead of a single
    previous-period comparison, because comparing to just the last data
    point is noisy: one low reading followed by a normal one looks like a
    "spike" even though nothing is wrong.

    The baseline center is the trailing window's MEDIAN, not its mean. A
    plain rolling mean is not robust to the very outliers it's meant to
    catch: one genuine spike sitting inside the window drags the mean up
    for every period afterward until it ages out, which then makes a run of
    perfectly normal values look like a "sudden decrease" relative to that
    contaminated average. The median barely moves when a small minority of
    points in the window are extreme, which is exactly why it's the
    standard choice for this ("robust statistics") over a straight mean.

    Adds:
        previous_value   -- immediately preceding period's value (kept for
                             display/debugging, not used for thresholding)
        baseline_median   -- median of metric_value over the trailing window
        history_count      -- how many periods are actually in that trailing
                             window, so callers can gate on
                             `metrics_engine.min_history_periods_for_stats`
                             and avoid alerting on a cold-start baseline
                             built from too little data.
    """
    baseline = read_table(cfg.state_table_full_name("metrics_baseline"))
    filtered = baseline.filter(F.col("metric_name") == metric_name)

    window_periods = cfg.metrics_engine.get("baseline_window_periods", 1)
    order_window = Window.partitionBy("table_name").orderBy("period_start")
    trailing_window = order_window.rowsBetween(-window_periods, -1)

    return (
        filtered.withColumn("previous_value", F.lag("metric_value").over(order_window))
        .withColumn(
            "baseline_median",
            F.expr("percentile_approx(metric_value, 0.5)").over(trailing_window),
        )
        .withColumn("history_count", F.count("metric_value").over(trailing_window))
    )


def _pct_change_vs_baseline_col() -> F.Column:
    """Percent deviation of the current period's value from the trailing
    rolling-median baseline (baseline_median), rather than from the single
    previous period or a mean the current value's own outliers could have
    dragged around."""
    return F.when(
        (F.col("baseline_median").isNotNull()) & (F.col("baseline_median") != 0),
        (F.col("metric_value") - F.col("baseline_median")) / F.abs(F.col("baseline_median")) * 100.0,
    ).otherwise(F.lit(None).cast("double"))


def _has_min_history_col(cfg: AppConfig) -> F.Column:
    min_history = cfg.metrics_engine.get("min_history_periods_for_stats", 1)
    return F.col("history_count") >= F.lit(min_history)


def _baseline_was_zero_now_nonzero_col() -> F.Column:
    """True when the trailing baseline was a clean zero (e.g. this column
    has had no nulls, or no duplicates, for the whole trailing window) and
    the current period isn't. Percent-change-from-baseline is mathematically
    undefined when the baseline is 0 (division by zero), but for a rate
    metric that's normally always zero -- null ratio, duplicate count --
    any nonzero reading at all is exactly the signal worth flagging, so this
    case needs an explicit fallback rather than silently never firing."""
    return (F.col("baseline_median") == 0) & (F.col("metric_value") > 0)

def _get_column_name_for_metric(cfg: AppConfig, metric_name: str) -> str:
    """Return the source column associated with a metric."""

    source_cfg = cfg.source_data

    # Example: sum_sales_amount -> sales_amount
    for column in source_cfg["numeric_metric_columns"]:
        if metric_name in {
            f"sum_{column}",
            f"avg_{column}",
            f"stddev_{column}",
            f"min_{column}",
            f"max_{column}",
            column,
        }:
            return column

    # Activity metric
    if metric_name == "sum_activity_count":
        return source_cfg["activity_column"]

    # Ingestion/staleness metric
    if metric_name == "last_updated_epoch":
        return source_cfg["ingestion_timestamp_column"]

    # Duplicate metric
    if metric_name == "duplicate_count":
        return ", ".join(source_cfg["duplicate_key_columns"])

    return None

def _detect_sudden_increase(cfg: AppConfig, run_id: str) -> DataFrame:
    rule = cfg.rule("sudden_increase")
    if not rule.get("enabled"):
        return _empty_log(get_spark())
    window_periods = cfg.metrics_engine.get("baseline_window_periods", 1)
    history = _metric_history_with_rolling_baseline(cfg, rule["metric"]).withColumn(
        "pct_change", _pct_change_vs_baseline_col()
    )
    hits = history.filter(_has_min_history_col(cfg) & (F.col("pct_change") >= F.lit(rule["threshold_pct"])))
    return hits.select(
        F.lit(str(uuid.uuid4())).alias("anomaly_id"),  # placeholder, overwritten below per-row
        "table_name",
        "period_start",
        F.col("metric_name"),
        F.col("metric_value").alias("detected_value"),
        F.col("baseline_median").alias("baseline_value"),
        F.col("pct_change"),
    ).withColumn("anomaly_id", F.expr("uuid()")).transform(
        lambda d: _finalize(
            d,
            run_id=run_id,
            anomaly_type="sudden_increase",
            severity=rule["severity"],
            message_expr=F.concat(
                F.lit("Sudden increase: "),
                F.col("metric_name"),
                F.lit(" is "),
                F.round(F.col("pct_change"), 1).cast("string"),
                F.lit(f"% above its trailing {window_periods}-period median (threshold "),
                F.lit(str(rule["threshold_pct"])),
                F.lit("%)"),
            ),
            threshold_desc=(
                f"pct_change_vs_rolling_median >= {rule['threshold_pct']}% "
                f"(trailing {window_periods} periods, min history "
                f"{cfg.metrics_engine.get('min_history_periods_for_stats', 1)})"
            ),
            column_name=_get_column_name_for_metric(cfg, rule["metric"]),
        )
    )


def _detect_sudden_decrease(cfg: AppConfig, run_id: str) -> DataFrame:
    rule = cfg.rule("sudden_decrease")
    if not rule.get("enabled"):
        return _empty_log(get_spark())
    window_periods = cfg.metrics_engine.get("baseline_window_periods", 1)
    history = _metric_history_with_rolling_baseline(cfg, rule["metric"]).withColumn(
        "pct_change", _pct_change_vs_baseline_col()
    )
    hits = history.filter(_has_min_history_col(cfg) & (F.col("pct_change") <= F.lit(rule["threshold_pct"])))
    return hits.select(
        "table_name",
        "period_start",
        F.col("metric_name"),
        F.col("metric_value").alias("detected_value"),
        F.col("baseline_median").alias("baseline_value"),
        F.col("pct_change"),
    ).transform(
        lambda d: _finalize(
            d,
            run_id=run_id,
            anomaly_type="sudden_decrease",
            severity=rule["severity"],
            message_expr=F.concat(
                F.lit("Sudden decrease: "),
                F.col("metric_name"),
                F.lit(" is "),
                F.round(F.col("pct_change"), 1).cast("string"),
                F.lit(f"% below its trailing {window_periods}-period median (threshold "),
                F.lit(str(rule["threshold_pct"])),
                F.lit("%)"),
            ),
            threshold_desc=(
                f"pct_change_vs_rolling_median <= {rule['threshold_pct']}% "
                f"(trailing {window_periods} periods, min history "
                f"{cfg.metrics_engine.get('min_history_periods_for_stats', 1)})"
            ),
            column_name=_get_column_name_for_metric(cfg, rule["metric"]),
        )
    )


def _detect_value_anomaly(cfg: AppConfig, run_id: str) -> DataFrame:
    """Row-level check against the source table: either a fixed business
    threshold or a Z-score computed from the metric's own historical mean/stddev."""
    rule = cfg.rule("value_anomaly")
    if not rule.get("enabled"):
        return _empty_log(get_spark())

    spark = get_spark()
    metric_col = rule["metric"]
    source_cfg = cfg.source_data
    src = read_table(cfg.source_table_full_name)

    if rule.get("use_fixed_threshold", True):
        upper = rule["fixed_upper_threshold"]
        lower = rule["fixed_lower_threshold"]
        hits = src.filter((F.col(metric_col) > F.lit(upper)) | (F.col(metric_col) < F.lit(lower)))
        threshold_desc = f"outside [{lower}, {upper}]"
        baseline_col = F.lit(None).cast("double")
    else:
        stats = src.agg(F.avg(metric_col).alias("mean"), F.stddev(metric_col).alias("std")).collect()[0]
        mean_val, std_val = stats["mean"], stats["std"]
        if not std_val:
            return _empty_log(spark)
        z_thresh = rule["zscore_threshold"]
        hits = src.withColumn("zscore", (F.col(metric_col) - F.lit(mean_val)) / F.lit(std_val)).filter(
            F.abs(F.col("zscore")) >= F.lit(z_thresh)
        )
        threshold_desc = f"|zscore| >= {z_thresh}"
        baseline_col = F.lit(float(mean_val))

    period_col = F.date_trunc(
        {"hourly": "hour", "daily": "day", "minute": "minute"}.get(cfg.metrics_engine["grain"], "hour"),
        F.col(source_cfg["timestamp_column"]),
    )
    result = hits.select(
        F.lit(cfg.source_table_full_name).alias("table_name"),
        period_col.alias("period_start"),
        F.lit(metric_col).alias("metric_name"),
        F.col(metric_col).cast("double").alias("detected_value"),
        baseline_col.alias("baseline_value"),
    )
    return result.transform(
        lambda d: _finalize(
            d,
            run_id=run_id,
            anomaly_type="value_anomaly",
            severity=rule["severity"],
            message_expr=F.concat(
                F.lit("Value anomaly on "),
                F.col("metric_name"),
                F.lit(" = "),
                F.col("detected_value").cast("string"),
                F.lit(f" ({threshold_desc})"),
            ),
            threshold_desc=threshold_desc,
            column_name=metric_col,
        )
    )


def _detect_missing_zero_activity(cfg: AppConfig, run_id: str) -> DataFrame:
    rule = cfg.rule("missing_zero_activity")
    if not rule.get("enabled"):
        return _empty_log(get_spark())
    history = _metric_history(cfg, rule["metric"])
    hits = history.filter(F.col("metric_value") == F.lit(rule["zero_activity_value"]))
    return hits.select(
        "table_name",
        "period_start",
        F.col("metric_name"),
        F.col("metric_value").alias("detected_value"),
        F.col("previous_value").alias("baseline_value"),
    ).transform(
        lambda d: _finalize(
            d,
            run_id=run_id,
            anomaly_type="missing_zero_activity",
            severity=rule["severity"],
            message_expr=F.concat(
                F.lit("No activity detected for period ("),
                F.col("metric_name"),
                F.lit(" = 0)"),
            ),
            threshold_desc=f"row_count == {rule['zero_activity_value']}",
            column_name=cfg.source_data["activity_column"],
        )
    )


def _detect_stale_data(cfg: AppConfig, run_id: str) -> DataFrame:
    rule = cfg.rule("stale_data")
    if not rule.get("enabled"):
        return _empty_log(get_spark())
    max_staleness_min = rule["max_allowed_staleness_minutes"]

    latest = (
        read_table(cfg.state_table_full_name("metrics_baseline"))
        .filter(F.col("metric_name") == "last_updated_epoch")
        .groupBy("table_name")
        .agg(F.max("metric_value").alias("last_updated_epoch"), F.max("period_start").alias("period_start"))
    )
    now_epoch = datetime.now(timezone.utc).timestamp()
    hits = latest.withColumn(
        "staleness_minutes", (F.lit(now_epoch) - F.col("last_updated_epoch")) / 60.0
    ).filter(F.col("staleness_minutes") > F.lit(max_staleness_min))

    return hits.select(
        "table_name",
        "period_start",
        F.lit("last_updated_epoch").alias("metric_name"),
        F.col("staleness_minutes").alias("detected_value"),
        F.lit(float(max_staleness_min)).alias("baseline_value"),
    ).transform(
        lambda d: _finalize(
            d,
            run_id=run_id,
            anomaly_type="stale_data",
            severity=rule["severity"],
            message_expr=F.concat(
                F.lit("Data is stale: last updated "),
                F.round(F.col("detected_value"), 1).cast("string"),
                F.lit(" minutes ago (limit "),
                F.lit(str(max_staleness_min)),
                F.lit(" min)"),
            ),
            threshold_desc=f"staleness_minutes > {max_staleness_min}",
            column_name=cfg.source_data["ingestion_timestamp_column"],
        )
    )


def _detect_duplicate_records(cfg: AppConfig, run_id: str) -> DataFrame:
    rule = cfg.rule("duplicate_record_anomaly")
    if not rule.get("enabled"):
        return _empty_log(get_spark())
    window_periods = cfg.metrics_engine.get("baseline_window_periods", 1)
    history = _metric_history_with_rolling_baseline(cfg, "duplicate_count").withColumn(
        "pct_change", _pct_change_vs_baseline_col()
    )
    has_history = _has_min_history_col(cfg)
    zero_to_nonzero = _baseline_was_zero_now_nonzero_col()
    # Cold start (not enough trailing periods yet to trust a baseline): fall
    # back to the absolute floor alone, same as a value_anomaly fixed
    # threshold. A clean (zero) baseline suddenly turning nonzero is always
    # a hit, since percent-change from zero is undefined. Otherwise, once
    # there's enough history and a nonzero baseline, require an actual
    # spike above the rolling median, not just any count above the floor.
    hits = history.filter(
        (F.col("metric_value") >= F.lit(rule["minimum_duplicate_count"]))
        & (~has_history | zero_to_nonzero | (F.col("pct_change") >= F.lit(rule["threshold_pct"])))
    )
    return hits.select(
        "table_name",
        "period_start",
        F.col("metric_name"),
        F.col("metric_value").alias("detected_value"),
        F.col("baseline_median").alias("baseline_value"),
    ).transform(
        lambda d: _finalize(
            d,
            run_id=run_id,
            anomaly_type="duplicate_record_anomaly",
            severity=rule["severity"],
            message_expr=F.concat(
                F.lit("Duplicate record spike: "),
                F.col("detected_value").cast("string"),
                F.lit(" duplicate row(s) detected"),
            ),
            threshold_desc=(
                f"duplicate_count >= {rule['minimum_duplicate_count']}, and "
                f"pct_change_vs_rolling_median >= {rule['threshold_pct']}% once "
                f"{cfg.metrics_engine.get('min_history_periods_for_stats', 1)}+ periods of "
                f"trailing {window_periods}-period history exist"
            ),
            column_name=", ".join(cfg.source_data["duplicate_key_columns"]),
        )
    )


def _detect_null_quality_spike(cfg: AppConfig, run_id: str) -> DataFrame:
    rule = cfg.rule("null_data_quality_spike")
    if not rule.get("enabled"):
        return _empty_log(get_spark())

    spark = get_spark()
    window_periods = cfg.metrics_engine.get("baseline_window_periods", 1)
    has_history = _has_min_history_col(cfg)
    zero_to_nonzero = _baseline_was_zero_now_nonzero_col()
    results = []
    for col in cfg.source_data["monitored_null_columns"]:
        metric_name = f"null_ratio_{col}"
        history = _metric_history_with_rolling_baseline(cfg, metric_name).withColumn(
            "pct_change", _pct_change_vs_baseline_col()
        )
        # Same cold-start / zero-baseline fallback as duplicate_record_anomaly:
        # without enough trailing history, or when this column has never had
        # a null before, fall back to the absolute floor alone.
        hits = history.filter(
            (F.col("metric_value") >= F.lit(rule["minimum_null_ratio"]))
            & (~has_history | zero_to_nonzero | (F.col("pct_change") >= F.lit(rule["threshold_pct"])))
        )
        results.append(
            hits.select(
                "table_name",
                "period_start",
                F.col("metric_name"),
                F.col("metric_value").alias("detected_value"),
                F.col("baseline_median").alias("baseline_value"),
            ).withColumn("column_name", F.lit(col))
        )

    if not results:
        return _empty_log(spark)
    unioned = results[0]
    for r in results[1:]:
        unioned = unioned.unionByName(r)

    return unioned.transform(
        lambda d: _finalize(
            d,
            run_id=run_id,
            anomaly_type="null_data_quality_spike",
            severity=rule["severity"],
            message_expr=F.concat(
                F.lit("Null/data-quality spike on "),
                F.col("column_name"),
                F.lit(": null ratio = "),
                F.round(F.col("detected_value") * 100, 1).cast("string"),
                F.lit("%"),
            ),
            threshold_desc=(
                f"null_ratio >= {rule['minimum_null_ratio']}, and "
                f"pct_change_vs_rolling_median >= {rule['threshold_pct']}% once "
                f"{cfg.metrics_engine.get('min_history_periods_for_stats', 1)}+ periods of "
                f"trailing {window_periods}-period history exist"
            ),
            column_name_from_data=True,
        )
    )


def _finalize(
    df: DataFrame,
    run_id: str,
    anomaly_type: str,
    severity: str,
    message_expr,
    threshold_desc: str,
    column_name: str = None,
    column_name_from_data: bool = False,
) -> DataFrame:
    out = df
    if "anomaly_id" not in out.columns:
        out = out.withColumn("anomaly_id", F.expr("uuid()"))
    out = (
        out.withColumn("run_id", F.lit(run_id))
        .withColumn("anomaly_type", F.lit(anomaly_type))
        .withColumn("severity", F.lit(severity))
        .withColumn("threshold_used", F.lit(threshold_desc))
        .withColumn("message", message_expr)
        .withColumn("detected_at", F.lit(datetime.now(timezone.utc)))
        .withColumn("alerted", F.lit(False))
    )
    if not column_name_from_data:
        out = out.withColumn("column_name", F.lit(column_name))
    return out.select(
        "anomaly_id",
        "run_id",
        "table_name",
        "column_name",
        "anomaly_type",
        "severity",
        "period_start",
        "metric_name",
        "detected_value",
        "baseline_value",
        "threshold_used",
        "message",
        "detected_at",
        "alerted",
    )


_DETECTORS: Dict[str, Callable[[AppConfig, str], DataFrame]] = {
    "sudden_increase": _detect_sudden_increase,
    "sudden_decrease": _detect_sudden_decrease,
    "value_anomaly": _detect_value_anomaly,
    "missing_zero_activity": _detect_missing_zero_activity,
    "stale_data": _detect_stale_data,
    "duplicate_record_anomaly": _detect_duplicate_records,
    "null_data_quality_spike": _detect_null_quality_spike,
}


def run_detection_engine(cfg: AppConfig) -> DataFrame:
    """Entry point used by the 02_Detect_Anomalies notebook. Evaluates every
    enabled anomaly type and appends all detections to anomaly_detection_log
    in a single write."""
    ensure_anomaly_log_table(cfg)
    spark = get_spark()
    run_id = str(uuid.uuid4())

    all_hits: List[DataFrame] = []
    for anomaly_type in cfg.enabled_anomaly_types():
        detector = _DETECTORS.get(anomaly_type)
        if detector is None:
            logger.warning("No detector implemented for configured anomaly type '%s'", anomaly_type)
            continue
        logger.info("Running detector: %s", anomaly_type)
        hits = detector(cfg, run_id)
        count = hits.count()
        logger.info("Detector '%s' found %d anomaly(ies)", anomaly_type, count)
        if count > 0:
            all_hits.append(hits)

    if not all_hits:
        logger.info("No anomalies detected in run %s", run_id)
        return _empty_log(spark)

    combined = all_hits[0]
    for frame in all_hits[1:]:
        combined = combined.unionByName(frame)

    raw_match_count = combined.count()
    combined = _drop_already_logged(cfg, combined)
    new_count = combined.count()
    already_logged_count = raw_match_count - new_count

    if new_count == 0:
        logger.info(
            "Run %s: %d anomaly match(es) found, but all %d were already logged in a previous run "
            "(same table/column/anomaly_type/period_start) -- nothing new to append. "
            "This is expected on repeated runs against unchanged source data; see "
            "04_Reset_Demo_State to clear state and start fresh, or check alert_log / "
            "anomaly_detection_log.alerted for anomalies still pending alert.",
            run_id,
            raw_match_count,
            already_logged_count,
        )
        return _empty_log(spark)

    logger.info(
        "Run %s: %d anomaly match(es) found (%d already logged and skipped, %d new)",
        run_id,
        raw_match_count,
        already_logged_count,
        new_count,
    )
    append_to_table(combined, cfg.state_table_full_name("anomaly_detection_log"))
    return combined


_DEDUPE_KEYS = ["table_name", "column_name", "anomaly_type", "period_start"]


def _drop_already_logged(cfg: AppConfig, hits: DataFrame) -> DataFrame:
    """Drops any newly-detected row that's an exact repeat (same table,
    column, anomaly type, and period) of something already sitting in
    anomaly_detection_log. Without this, re-running detection against
    unchanged source data re-appends -- and therefore re-alerts on -- the
    same anomaly every time. Nulls in column_name (table-level metrics) are
    handled with a sentinel so they dedupe correctly instead of always
    comparing unequal, as raw SQL null semantics would."""
    existing = (
        read_table(cfg.state_table_full_name("anomaly_detection_log"))
        .select(*_DEDUPE_KEYS)
        .withColumn("column_name", F.coalesce(F.col("column_name"), F.lit("__NULL__")))
        .distinct()
    )
    hits_keyed = hits.withColumn(
        "_column_name_key", F.coalesce(F.col("column_name"), F.lit("__NULL__"))
    )
    existing_keyed = existing.withColumnRenamed("column_name", "_column_name_key")
    return hits_keyed.join(
        existing_keyed,
        on=["table_name", "_column_name_key", "anomaly_type", "period_start"],
        how="left_anti",
    ).drop("_column_name_key")
