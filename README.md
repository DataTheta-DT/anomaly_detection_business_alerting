# Anomaly Detection & Business Alerting — Solution Accelerator

A Databricks / Unity Catalog accelerator that watches your data, detects anomalies, and automatically sends business alerts (Email) when something looks wrong.

Everything you'd need to change to reuse this project — catalog, tables, thresholds, email settings — lives in **one file**: `config/config.json`. No code changes needed to reuse it elsewhere.

## What it detects

| # | Anomaly | What it means |
|---|---|---|
| 1 | Sudden Increase | A metric jumps sharply above its normal range |
| 2 | Sudden Decrease | A metric drops sharply below its normal range |
| 3 | Value Anomaly | A single row breaks a set threshold |
| 4 | Missing / Zero Activity | Expected data didn't arrive at all |
| 5 | Stale Data | Data hasn't refreshed in the expected time |
| 6 | Duplicate Record Anomaly | Too many duplicate records show up |
| 7 | Null / Data Quality Spike | A sudden rise in missing/blank values |

### How the trend checks work

Sudden Increase/Decrease, Duplicate, and Null checks compare each period against a **rolling median baseline** (trailing window, default 24 periods) instead of just the last data point. Median is used instead of average because a single spike can drag an average off course for a long time — median stays stable. A check only fires once there's enough history (default 5 periods), so it won't misfire on a cold start.

## Architecture

```
Source Table
     │
     ▼
01_Compute_Metrics   →  metrics_baseline (metric history)
     │
     ▼
02_Detect_Anomalies  →  anomaly_detection_log (all 7 checks)
     │
     ▼
03_Trigger_Alerts    →  alert_log + Email
```

Each stage writes to its own Delta table, so every step can be re-run independently and safely — no duplicate data, no duplicate alerts by default (an `alerted` flag prevents re-sending). Set `alerting.dedupe_alerts` to `false` in `config.json` to disable this and re-send alerts for every logged anomaly on every run (useful for demos/testing; not recommended for production, since it will re-alert on the same anomaly indefinitely).

## Project structure

```
anomaly_detection_business_alerting/
├── config/
│   └── config.json           # the only file you edit to reuse this project
├── src/anomaly_alerting/
│   ├── config.py               # loads config.json
│   ├── metrics.py              # Metrics Engine
│   ├── detection.py            # Anomaly Detection Engine (7 checks)
│   ├── alerting.py             # Business Alerting Engine
│   └── utils.py / logger.py    # shared helpers
├── notebooks/
│   ├── 00_Setup.py             # creates tables, seeds rules, loads sample data
│   ├── 01_Compute_Metrics.py   # builds metric history
│   ├── 02_Detect_Anomalies.py  # runs all anomaly checks
│   └── 03_Trigger_Alerts.py    # sends alerts
├── data/
│   └── anomaly_detection_sample_50_rows.csv
└── requirements.txt
```

## Configuration (`config/config.json`)

| Section | Controls |
|---|---|
| `unity_catalog` | Catalog, schema, source table name |
| `state_tables` | Names of the framework's Delta tables |
| `source_data` | Which columns to monitor for nulls / duplicates / values |
| `metrics_engine` | Baseline window size, minimum history needed |
| `anomaly_rules` | Per-anomaly: enabled, metric, threshold, severity |
| `severity_routing` | Which channels fire for each severity |
| `alerting.email` | SMTP settings, recipients |

Secrets (SMTP password) are **never** stored in `config.json` — they're pulled at runtime from a Databricks secret scope via `dbutils.secrets.get`.

## Prerequisites

- A Unity Catalog–enabled Databricks workspace
- `CREATE SCHEMA` / `CREATE TABLE` privileges on the configured catalog
- A Databricks secret scope for your SMTP password:

  ```bash
  databricks secrets create-scope anomaly_alerting
  databricks secrets put-secret anomaly_alerting smtp_password
  ```

## Setup

1. **Upload this project** into your Databricks workspace (Repos or Workspace Import), keeping the folder structure intact.
2. **Edit `config/config.json`** — set your catalog, schema, table names, and email sender/recipients.
3. **Create the secret** as shown above.
4. **Run the notebooks in order**, on any cluster or Serverless compute:
   - `00_Setup.py` — creates schemas/tables, seeds rules, loads sample data
   - `01_Compute_Metrics.py` — builds metric history
   - `02_Detect_Anomalies.py` — runs all anomaly checks
   - `03_Trigger_Alerts.py` — sends alerts and logs the outcome
5. **Schedule it** — set up `01 → 02 → 03` as a Databricks Job (hourly/daily). Re-run `00_Setup` only when you register a new table or change the rules. All steps are safe to re-run.

## Extending to a new table

1. Add the table's columns to `source_data` in `config.json`.
2. Adjust thresholds under `anomaly_rules`.
3. No code changes needed — everything is driven by config.
