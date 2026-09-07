# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04_Reset_Demo_State
# MAGIC
# MAGIC **Why this notebook exists:** `01_Compute_Metrics` and `02_Detect_Anomalies`
# MAGIC are idempotent by design -- re-running them against unchanged source data
# MAGIC will *not* produce new anomalies or new alerts, because:
# MAGIC - `metrics_baseline` is upserted per `(table_name, period_start, metric_name)`
# MAGIC - `anomaly_detection_log` dedupes new detections against existing rows keyed
# MAGIC   on `(table_name, column_name, anomaly_type, period_start)`
# MAGIC - `03_Trigger_Alerts` only alerts on rows where `alerted = false`
# MAGIC
# MAGIC This is intentional in production (it stops the same anomaly from
# MAGIC re-alerting on every scheduled run). But if you're demoing or testing
# MAGIC against the same static sample CSV and want to see the pipeline detect
# MAGIC and alert from scratch again, use this notebook to clear state first.
# MAGIC
# MAGIC **This truncates `metrics_baseline`, `anomaly_detection_log`, and
# MAGIC `alert_log`.** It does NOT touch `anomaly_rules_config` or your source
# MAGIC table. Requires typing `RESET` in the widget below to run.

# COMMAND ----------

import sys

sys.path.append("../src")

from anomaly_alerting.config import load_config
from anomaly_alerting.logger import get_logger
from anomaly_alerting.utils import get_spark, table_exists

# COMMAND ----------

CONFIG_PATH = "../config/config.json"
cfg = load_config(CONFIG_PATH)
logger = get_logger(__name__, cfg.log_level)

dbutils.widgets.text("confirm", "", "Type RESET to confirm truncating state tables")

# COMMAND ----------

confirm = dbutils.widgets.get("confirm")

if confirm != "RESET":
    dbutils.notebook.exit(
        "Nothing was reset. Set the 'confirm' widget to RESET and re-run this notebook to proceed."
    )

# COMMAND ----------

spark = get_spark()
tables_to_reset = ["metrics_baseline", "anomaly_detection_log", "alert_log"]

for logical_name in tables_to_reset:
    full_name = cfg.state_table_full_name(logical_name)
    if table_exists(full_name):
        spark.sql(f"TRUNCATE TABLE {full_name}")
        logger.info("Truncated %s", full_name)
    else:
        logger.info("Skipped %s (does not exist yet)", full_name)

logger.info(
    "Demo state reset complete. Re-run 01_Compute_Metrics -> 02_Detect_Anomalies -> "
    "03_Trigger_Alerts to see fresh detections and alerts."
)