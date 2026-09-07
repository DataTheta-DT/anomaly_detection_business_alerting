# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02_Detect_Anomalies
# MAGIC Reads per-metric rule definitions from `config/config.json` and evaluates
# MAGIC each of the seven anomaly types against the current and historical rows in
# MAGIC `metrics_baseline` (and, for value anomalies, the source table itself),
# MAGIC writing every detection with its severity and detected value to
# MAGIC `anomaly_detection_log`.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import sys

sys.path.append("../src")

from anomaly_alerting.config import load_config
from anomaly_alerting.logger import get_logger
from anomaly_alerting.detection import run_detection_engine

# COMMAND ----------

CONFIG_PATH = "../config/config.json"
cfg = load_config(CONFIG_PATH)
logger = get_logger(__name__, cfg.log_level)

logger.info("Enabled anomaly types: %s", cfg.enabled_anomaly_types())

# COMMAND ----------

detections = run_detection_engine(cfg)
logger.info("Detection run complete: %d anomaly(ies) found", detections.count())

# COMMAND ----------

display(detections.orderBy("severity", "detected_at"))