# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01_Compute_Metrics
# MAGIC Computes per-table/per-column business and pipeline metrics (row counts,
# MAGIC null ratios, duplicate-key counts, min/max/mean/stddev, last-updated
# MAGIC timestamp) for every registered table, and appends them to
# MAGIC `<CATALOG>.<CONFIG_SCHEMA>.metrics_baseline`.
# MAGIC
# MAGIC Run this before `02_Detect_Anomalies` so every anomaly check has a current
# MAGIC and historical baseline to compare against. Typically scheduled hourly or
# MAGIC nightly.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import sys

sys.path.append("../src")

from anomaly_alerting.config import load_config
from anomaly_alerting.logger import get_logger
from anomaly_alerting.metrics import run_metrics_engine

# COMMAND ----------

CONFIG_PATH = "../config/config.json"
cfg = load_config(CONFIG_PATH)
logger = get_logger(__name__, cfg.log_level)

# COMMAND ----------

rows_written = run_metrics_engine(cfg)
logger.info("Metrics engine wrote %d row(s) to metrics_baseline", rows_written)

# COMMAND ----------

display(spark.table(cfg.state_table_full_name("metrics_baseline")).orderBy("period_start", "metric_name"))