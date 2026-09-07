# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03_Trigger_Alerts
# MAGIC Reads new, un-alerted rows from `anomaly_detection_log`, formats a
# MAGIC business-readable message per anomaly, dispatches it to the channel(s)
# MAGIC configured for its severity (email / Slack / Teams webhook), and records
# MAGIC the outcome in `alert_log`. Also retires config rows for tables that no
# MAGIC longer exist (self-heal).
# MAGIC
# MAGIC **Secrets required** (see README for setup commands):
# MAGIC - `anomaly_alerting.smtp_password` — the sender mailbox's app password
# MAGIC - `anomaly_alerting.slack_webhook_url` — only if Slack is enabled
# MAGIC - `anomaly_alerting.teams_webhook_url` — only if Teams is enabled

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import sys

sys.path.append("../src")

from anomaly_alerting.config import load_config
from anomaly_alerting.logger import get_logger
from anomaly_alerting.alerting import run_alerting_engine, retire_stale_config_rows

# COMMAND ----------

CONFIG_PATH = "../config/config.json"
cfg = load_config(CONFIG_PATH)
logger = get_logger(__name__, cfg.log_level)

# COMMAND ----------



# COMMAND ----------

alert_results = run_alerting_engine(cfg)
retired = retire_stale_config_rows(cfg)
logger.info("Retired %d stale config row(s)", retired)

# COMMAND ----------

display(alert_results)

