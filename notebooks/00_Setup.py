# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00_Setup
# MAGIC Creates the framework's Unity Catalog state tables (`anomaly_rules_config`,
# MAGIC `metrics_baseline`, `anomaly_detection_log`, `alert_log`) under
# MAGIC `<CATALOG>.<CONFIG_SCHEMA>`, seeds `anomaly_rules_config` from `config.json`,
# MAGIC and (for this sample project) loads the sample CSV into the configured
# MAGIC source table.
# MAGIC
# MAGIC Run once, and again any time you register a new table or change the rules
# MAGIC in `config/config.json`.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import sys

sys.path.append("../src")

from pyspark.sql.types import StructType

from anomaly_alerting.config import load_config
from anomaly_alerting.logger import get_logger
from anomaly_alerting.utils import ensure_schema_exists, get_spark, table_exists
from anomaly_alerting.metrics import METRICS_BASELINE_SCHEMA, ensure_metrics_baseline_table
from anomaly_alerting.detection import ANOMALY_LOG_SCHEMA, ensure_anomaly_log_table
from anomaly_alerting.alerting import ALERT_LOG_SCHEMA, ensure_alert_log_table

# COMMAND ----------

CONFIG_PATH = "../config/config.json"  # update this path only if you relocate the config file
cfg = load_config(CONFIG_PATH)
logger = get_logger(__name__, cfg.log_level)
spark = get_spark()

logger.info("Environment: %s", cfg.environment_name)
logger.info("Catalog: %s | Config schema: %s | Source table: %s", cfg.catalog, cfg.config_schema, cfg.source_table_full_name)

# COMMAND ----------

# MAGIC %md ### 1. Create catalog schemas

# COMMAND ----------

ensure_schema_exists(cfg.catalog, cfg.config_schema)
ensure_schema_exists(cfg.catalog, cfg.source_schema)

# COMMAND ----------

# MAGIC %md ### 2. Create framework state tables (idempotent)

# COMMAND ----------

ensure_metrics_baseline_table(cfg)
ensure_anomaly_log_table(cfg)
ensure_alert_log_table(cfg)

# COMMAND ----------

# MAGIC %md ### 3. Seed `anomaly_rules_config` from config.json
# MAGIC The rules table is a queryable, auditable mirror of `config.json`'s
# MAGIC `anomaly_rules` section -- useful for BI dashboards and change history.
# MAGIC The pipeline itself reads thresholds from config.json directly; this table
# MAGIC is for visibility/audit, not runtime lookups.

# COMMAND ----------

import json

rules_table = cfg.state_table_full_name("anomaly_rules_config")
rules_rows = [
    {
        "table_name": cfg.source_table_full_name,
        "anomaly_type": anomaly_type,
        "enabled": rule_cfg.get("enabled", False),
        "severity": rule_cfg.get("severity"),
        "rule_json": json.dumps(rule_cfg),
    }
    for anomaly_type, rule_cfg in cfg.anomaly_rules.items()
]

rules_schema = StructType.fromJson(
    {
        "type": "struct",
        "fields": [
            {"name": "table_name", "type": "string", "nullable": False, "metadata": {}},
            {"name": "anomaly_type", "type": "string", "nullable": False, "metadata": {}},
            {"name": "enabled", "type": "boolean", "nullable": False, "metadata": {}},
            {"name": "severity", "type": "string", "nullable": True, "metadata": {}},
            {"name": "rule_json", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
)

rules_df = spark.createDataFrame(rules_rows, schema=rules_schema)
rules_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(rules_table)
logger.info("Seeded %s with %d rule(s)", rules_table, rules_df.count())
display(rules_df)

# COMMAND ----------

# MAGIC %md Setup complete. Proceed to `01_Compute_Metrics`.