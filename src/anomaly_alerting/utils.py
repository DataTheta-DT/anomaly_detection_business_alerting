"""
utils.py
--------
Generic, project-agnostic helper functions: Spark session access, Delta table
create-if-not-exists, safe table reads, retry decorator, and secret lookup.

These helpers know nothing about anomaly detection specifically -- they can be
lifted into any other Databricks accelerator unchanged.
"""

from __future__ import annotations

import functools
import os
import time
from typing import Any, Callable, List, Optional, TypeVar

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

# Storage format for all framework tables. Always "delta" in a real Databricks
# workspace. Overridable via env var only for offline local unit testing
# (see tests/test_detection.py), where Delta's Maven jars aren't reachable.
_TABLE_FORMAT = os.environ.get("ANOMALY_ALERTING_TABLE_FORMAT", "delta")


def get_spark() -> SparkSession:
    """Returns the active SparkSession. On Databricks (including Serverless)
    this is always available via getActiveSession(); the getOrCreate() fallback
    only matters for local unit tests."""
    session = SparkSession.getActiveSession()
    if session is not None:
        return session
    return SparkSession.builder.appName("anomaly_detection_business_alerting").getOrCreate()


def table_exists(full_table_name: str) -> bool:
    spark = get_spark()
    try:
        return spark.catalog.tableExists(full_table_name)
    except Exception:  # pragma: no cover - defensive: older DBR without catalog.tableExists
        try:
            spark.table(full_table_name)
            return True
        except Exception:
            return False


def ensure_schema_exists(catalog: str, schema: str) -> None:
    spark = get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    logger.info("Ensured schema exists: %s.%s", catalog, schema)


def create_table_if_not_exists(full_table_name: str, schema: StructType) -> None:
    """Creates an empty managed Delta table if it does not already exist."""
    spark = get_spark()

    if table_exists(full_table_name):
        logger.debug("Table already exists, skipping create: %s", full_table_name)
        return

    empty_df = spark.createDataFrame([], schema)

    # Use append mode because the table has already been checked
    # and does not exist.
    empty_df.write.format(_TABLE_FORMAT).mode("append").saveAsTable(full_table_name)

    logger.info("Created table: %s", full_table_name)


def read_table(full_table_name: str) -> DataFrame:
    spark = get_spark()
    logger.debug("Reading table: %s", full_table_name)
    return spark.table(full_table_name)


def append_to_table(df: DataFrame, full_table_name: str) -> int:
    row_count = df.count()
    df.write.format(_TABLE_FORMAT).mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)
    logger.info("Appended %d row(s) to %s", row_count, full_table_name)
    return row_count


def overwrite_table(df: DataFrame, full_table_name: str) -> int:
    row_count = df.count()
    df.write.format(_TABLE_FORMAT).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        full_table_name
    )
    logger.info("Overwrote %s with %d row(s)", full_table_name, row_count)
    return row_count


def merge_upsert(
    df: DataFrame,
    full_table_name: str,
    merge_keys: List[str],
    update_columns: Optional[List[str]] = None,
    insert_when_not_matched: bool = True,
) -> None:
    """Upserts df into an existing table on merge_keys.

    Uses Delta's native MERGE INTO in production. When running under the
    parquet fallback format (offline local testing only), falls back to a
    read/replace/overwrite emulation since parquet has no MERGE support.

    insert_when_not_matched:
        True (default) matches the historical behavior: unmatched source
        rows are inserted via whenNotMatchedInsertAll(), which requires df
        to carry every target column (a genuine full-schema upsert, e.g.
        metrics.py's _write_metrics, where a brand-new period really should
        be inserted).
        Set this to False for narrow, partial-column updates (df has only
        merge_keys + update_columns, e.g. alerting.py's mark-alerted call)
        where new rows are never expected. Delta validates the INSERT
        clause's column list at query-analysis time regardless of whether
        any row would actually go unmatched, so leaving this True with a
        narrow df fails immediately with DELTA_MERGE_UNRESOLVED_EXPRESSION
        even when zero rows are actually unmatched.
    """
    spark = get_spark()

    if _TABLE_FORMAT != "delta":
        existing = read_table(full_table_name)
        non_key_cols = update_columns or [c for c in df.columns if c not in merge_keys]
        updates_only = df.select(*merge_keys, *non_key_cols)
        join_cond = [existing[k] == updates_only[k] for k in merge_keys]

        unmatched_existing = existing.join(updates_only, join_cond, "left_anti").select(*existing.columns)
        matched_updated = existing.join(updates_only, join_cond, "inner").select(
            *[F_col(existing, updates_only, c, update_columns) for c in existing.columns]
        )
        merged = unmatched_existing.unionByName(matched_updated)

        # whenNotMatchedInsertAll() equivalent: source rows with no existing
        # match at all are new records and need inserting, not just
        # updating. Only possible when the source carries every target
        # column (true for a full-schema upsert like metrics_baseline; not
        # true for a narrow partial-column update like alerting.py's
        # mark-alerted call, where "not matched" should never occur anyway
        # since it only updates rows already written by the detection
        # engine -- so this is skipped with a warning rather than failing).
        if set(existing.columns).issubset(set(df.columns)):
            new_rows = df.select(*existing.columns).join(
                existing.select(*merge_keys), merge_keys, "left_anti"
            )
            merged = merged.unionByName(new_rows)
        elif df.join(existing.select(*merge_keys), merge_keys, "left_anti").limit(1).count() > 0:
            logger.warning(
                "merge_upsert (parquet fallback) into %s: source has row(s) with no "
                "existing match, but lacks the full target schema needed to insert "
                "them -- skipping those rows.",
                full_table_name,
            )

        # merged's lineage still traces back to `existing`, which was read
        # from full_table_name -- Spark refuses to overwrite a table that's
        # also a read source in the same plan (UNSUPPORTED_OVERWRITE).
        # localCheckpoint materializes the result and drops that lineage,
        # so the subsequent overwrite targets a plan that no longer
        # references the table it's about to replace.
        merged = merged.localCheckpoint(eager=True)

        overwrite_table(merged, full_table_name)
        logger.info("Merged (parquet fallback) into %s on keys %s", full_table_name, merge_keys)
        return

    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, full_table_name)
    condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    merger = target.alias("target").merge(df.alias("source"), condition)
    if update_columns:
        set_expr = {c: f"source.{c}" for c in update_columns}
        merger = merger.whenMatchedUpdate(set=set_expr)
    else:
        merger = merger.whenMatchedUpdateAll()
    if insert_when_not_matched:
        merger = merger.whenNotMatchedInsertAll()
    merger.execute()
    logger.info("Merged into %s on keys %s", full_table_name, merge_keys)


def F_col(existing: DataFrame, updates: DataFrame, col: str, update_columns: Optional[List[str]]):
    if update_columns and col in update_columns:
        return updates[col]
    return existing[col]


def get_secret(scope: str, key: str) -> str:
    """Fetches a secret from Databricks secret scopes. Falls back to raising a
    clear error rather than silently returning an empty string, since a blank
    SMTP password or webhook URL fails alerting in a hard-to-diagnose way."""
    try:
        dbutils = _get_dbutils()
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:
        raise RuntimeError(
            f"Could not read secret '{key}' from scope '{scope}'. "
            f"Create it with: databricks secrets put-secret {scope} {key}"
        ) from exc


def _get_dbutils():
    """Retrieves dbutils in a notebook-safe way. Works when this module is
    imported from a Databricks notebook, where dbutils is injected into globals."""
    try:
        import IPython

        ip = IPython.get_ipython()
        if ip is not None and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"]
    except ImportError:
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(get_spark())
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("dbutils is not available in this environment") from exc


def retry(max_attempts: int = 3, backoff_seconds: float = 5.0) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that retries a function on exception with linear backoff.
    Used for network calls (SMTP, webhooks) that can fail transiently."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - intentional broad retry
                    last_exc = exc
                    logger.warning(
                        "Attempt %d/%d for %s failed: %s", attempt, max_attempts, func.__name__, exc
                    )
                    if attempt < max_attempts:
                        time.sleep(backoff_seconds)
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator
