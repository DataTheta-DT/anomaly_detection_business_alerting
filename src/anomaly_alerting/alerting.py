"""
alerting.py
-----------
Business Alerting Engine: reads new, un-alerted rows from
anomaly_detection_log, formats a business-readable message per anomaly,
dispatches it to the channel(s) configured for its severity (email / Slack /
Teams webhook), and records the outcome in alert_log. Marks each anomaly row
`alerted = true` once at least one channel has succeeded, so re-runs never
double-send.
"""

from __future__ import annotations

import smtplib
import uuid
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import AppConfig
from .logger import get_logger
from .utils import (
    append_to_table,
    create_table_if_not_exists,
    get_secret,
    get_spark,
    merge_upsert,
    read_table,
    retry,
)

logger = get_logger(__name__)

ALERT_LOG_SCHEMA = StructType(
    [
        StructField("alert_id", StringType(), False),
        StructField("anomaly_id", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("sent_at", TimestampType(), False),
    ]
)


def ensure_alert_log_table(cfg: AppConfig) -> None:
    create_table_if_not_exists(cfg.state_table_full_name("alert_log"), ALERT_LOG_SCHEMA)


def _get_unalerted_anomalies(cfg: AppConfig) -> DataFrame:
    log = read_table(cfg.state_table_full_name("anomaly_detection_log"))
    if cfg.alerting.get("dedupe_alerts", True):
        return log.filter(F.col("alerted") == F.lit(False))
    # dedupe_alerts = false: re-alert on every logged anomaly on every run,
    # regardless of whether it was already alerted on before.
    return log


def _format_email_body(row: Dict[str, Any]) -> str:
    return (
        f"<h3>Anomaly Detected</h3>"
        f"<table cellpadding='6' cellspacing='0' border='1' style='border-collapse:collapse'>"
        f"<tr><td><b>Table</b></td><td>{row['table_name']}</td></tr>"
        f"<tr><td><b>Column</b></td><td>{row.get('column_name') or '-'}</td></tr>"
        f"<tr><td><b>Anomaly Type</b></td><td>{row['anomaly_type']}</td></tr>"
        f"<tr><td><b>Severity</b></td><td>{row['severity']}</td></tr>"
        f"<tr><td><b>Metric</b></td><td>{row.get('metric_name') or '-'}</td></tr>"
        f"<tr><td><b>Detected Value</b></td><td>{row.get('detected_value')}</td></tr>"
        f"<tr><td><b>Baseline Value</b></td><td>{row.get('baseline_value')}</td></tr>"
        f"<tr><td><b>Threshold</b></td><td>{row.get('threshold_used')}</td></tr>"
        f"<tr><td><b>Period</b></td><td>{row.get('period_start')}</td></tr>"
        f"<tr><td><b>Message</b></td><td>{row['message']}</td></tr>"
        f"<tr><td><b>Detected At</b></td><td>{row['detected_at']}</td></tr>"
        f"</table>"
    )


def _df_to_html_table(df: pd.DataFrame) -> str:
    """Renders a DataFrame as a styled HTML table (yellow header, bordered
    cells) to append below the main email body."""
    style = (
        "<style>#tableformatting table, table th, table td "
        "{ font-size:10pt; border:1px solid black; border-collapse:collapse; "
        "text-align:left; padding: 5px; } thead {background-color: yellow}</style>"
    )
    return f"{style}<p></p><div id='tableformatting'>{df.to_html(index=False)}</div>"


@retry(max_attempts=3, backoff_seconds=5)
def send_email_smtp(
    cfg: AppConfig,
    subject: str,
    html_body: str,
    send_to: Optional[List[str]] = None,
    send_cc: Optional[List[str]] = None,
    df_body: Optional[pd.DataFrame] = None,
) -> None:
    """Sends an HTML email through the team's standard mailbox.

    This is the `send_email_smtp` pattern the team uses, adapted to this
    project's config/secret handling:
      - smtp_host / smtp_port / sender_email come from config.json's
        `alerting.email` block (currently smtp.office365.com:587, sender
        satish.harkal@datatheta.com) -- no per-call SMTP details needed.
      - The mailbox password is read from the Databricks secret scope
        (`anomaly_alerting.smtp_password` by default) via `get_secret`,
        never hardcoded in source. Set it once with:
            databricks secrets put-secret anomaly_alerting smtp_password
      - `send_to` / `send_cc` accept a list of addresses and default to the
        `recipients` / `cc` lists in config.json when omitted.
      - `df_body`, if given, is rendered as a styled HTML table and appended
        to `html_body` before sending.
    """
    email_cfg = cfg.alerting["email"]
    password = get_secret(email_cfg["sender_password_secret_scope"], email_cfg["sender_password_secret_key"])

    to_list = list(send_to) if send_to else list(email_cfg["recipients"])
    cc_list = list(send_cc) if send_cc else list(email_cfg.get("cc", []))

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_cfg["sender_email"]
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    message_body = html_body + (_df_to_html_table(df_body) if df_body is not None else "")
    msg.attach(MIMEText(message_body, "html"))

    all_recipients = to_list + cc_list

    with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
        if email_cfg.get("use_tls", True):
            server.starttls()
        server.login(email_cfg["sender_email"], password)
        server.sendmail(email_cfg["sender_email"], all_recipients, msg.as_string())

    logger.info("Email sent: '%s' to %s (cc: %s)", subject, to_list, cc_list)


@retry(max_attempts=3, backoff_seconds=5)
def _send_slack(cfg: AppConfig, text: str) -> None:
    slack_cfg = cfg.alerting["slack"]
    webhook_url = get_secret(slack_cfg["webhook_url_secret_scope"], slack_cfg["webhook_url_secret_key"])
    payload = {"channel": slack_cfg.get("channel"), "text": text}
    response = requests.post(webhook_url, json=payload, timeout=15)
    response.raise_for_status()
    logger.info("Slack alert sent to %s", slack_cfg.get("channel"))


@retry(max_attempts=3, backoff_seconds=5)
def _send_teams(cfg: AppConfig, title: str, text: str) -> None:
    teams_cfg = cfg.alerting["teams"]
    webhook_url = get_secret(teams_cfg["webhook_url_secret_scope"], teams_cfg["webhook_url_secret_key"])
    payload = {"@type": "MessageCard", "@context": "http://schema.org/extensions", "title": title, "text": text}
    response = requests.post(webhook_url, json=payload, timeout=15)
    response.raise_for_status()
    logger.info("Teams alert sent")


def _format_digest_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """One row of the digest table -- plain strings/values only, no HTML,
    since this feeds a pandas DataFrame rendered by _df_to_html_table."""
    return {
        "Severity": row["severity"],
        "Table": row["table_name"],
        "Column": row.get("column_name") or "-",
        "Anomaly Type": row["anomaly_type"],
        "Metric": row.get("metric_name") or "-",
        "Detected Value": row.get("detected_value"),
        "Baseline Value": row.get("baseline_value"),
        "Message": row["message"],
        "Detected At": row["detected_at"],
    }


def _send_email_digest(cfg: AppConfig, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sends ONE email covering every anomaly in `rows`, as a summary table,
    instead of one email per anomaly. Returns an outcome dict shaped like
    _dispatch_channel's, so callers can handle it the same way."""
    email_cfg = cfg.alerting["email"]
    digest_template = email_cfg.get(
        "digest_subject_template", "[Anomaly Digest] {count} anomalies detected - {run_date}"
    )
    subject = digest_template.format(count=len(rows), run_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    sorted_rows = sorted(rows, key=lambda r: severity_order.get(r["severity"], 99))
    by_severity: Dict[str, int] = {}
    for r in rows:
        by_severity[r["severity"]] = by_severity.get(r["severity"], 0) + 1
    severity_summary = ", ".join(
        f"{count} {sev}" for sev, count in sorted(by_severity.items(), key=lambda kv: severity_order.get(kv[0], 99))
    )

    html_body = f"<h3>Anomaly Digest</h3><p>{len(rows)} anomaly(ies) detected in this run ({severity_summary}).</p>"
    df_body = pd.DataFrame([_format_digest_row(r) for r in sorted_rows])

    try:
        send_email_smtp(cfg, subject, html_body, df_body=df_body)
        return dict(status="SENT", error_message=None)
    except Exception as exc:  # noqa: BLE001 - alerting must never crash the pipeline
        logger.error("Failed to send digest email for %d anomalies: %s", len(rows), exc)
        return dict(status="FAILED", error_message=str(exc))


def _dispatch_channel(cfg: AppConfig, channel: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """Sends one anomaly row to one channel. Returns an alert_log record. Never
    raises -- failures are captured as FAILED rows so one bad channel can't
    stop the rest of the run."""
    subject_template = cfg.alerting["email"]["subject_template"]
    subject = subject_template.format(
        severity=row["severity"], anomaly_type=row["anomaly_type"], table_name=row["table_name"]
    )
    try:
        if channel == "email" and cfg.alerting["email"].get("enabled"):
            send_email_smtp(cfg, subject, _format_email_body(row))
        elif channel == "slack" and cfg.alerting["slack"].get("enabled"):
            _send_slack(cfg, f"*{subject}*\n{row['message']}")
        elif channel == "teams" and cfg.alerting["teams"].get("enabled"):
            _send_teams(cfg, subject, row["message"])
        else:
            return dict(status="SKIPPED", error_message=f"Channel '{channel}' disabled in config")
        return dict(status="SENT", error_message=None)
    except Exception as exc:  # noqa: BLE001 - alerting must never crash the pipeline
        logger.error("Failed to send %s alert for anomaly %s: %s", channel, row.get("anomaly_id"), exc)
        return dict(status="FAILED", error_message=str(exc))


def run_alerting_engine(cfg: AppConfig) -> DataFrame:
    """Entry point used by the 03_Trigger_Alerts notebook."""
    ensure_alert_log_table(cfg)
    spark = get_spark()

    unalerted = _get_unalerted_anomalies(cfg)
    rows = [r.asDict() for r in unalerted.collect()]

    if not rows:
        full_log = read_table(cfg.state_table_full_name("anomaly_detection_log"))
        total_logged = full_log.count()
        total_alerted = full_log.filter(F.col("alerted") == True).count()  # noqa: E712
        logger.info(
            "No new anomalies to alert on. (%d anomaly(ies) total in anomaly_detection_log, "
            "%d already alerted, 0 pending.) If you expected fresh alerts, note that detection "
            "dedupes on (table, column, anomaly_type, period_start) -- re-running against "
            "unchanged source data won't produce new anomalies to alert on. Use "
            "04_Reset_Demo_State to clear state and re-run from scratch.",
            total_logged,
            total_alerted,
        )
        return spark.createDataFrame([], ALERT_LOG_SCHEMA)

    alert_records: List[Dict[str, Any]] = []
    alerted_anomaly_ids = set()

    send_digest = cfg.alerting["email"].get("enabled") and cfg.alerting["email"].get("send_digest")
    digest_candidates: List[Dict[str, Any]] = []  # rows whose severity routes to email, held back for the digest

    for row in rows:
        channels = cfg.channels_for_severity(row["severity"])
        if not channels:
            logger.warning("No channels configured for severity '%s'; skipping alert.", row["severity"])
            continue
        any_success = False
        for channel in channels:
            if channel == "email" and send_digest:
                digest_candidates.append(row)
                continue  # sent as part of the single digest email below, not individually
            outcome = _dispatch_channel(cfg, channel, row)
            alert_records.append(
                dict(
                    alert_id=str(uuid.uuid4()),
                    anomaly_id=row["anomaly_id"],
                    channel=channel,
                    status=outcome["status"],
                    error_message=outcome["error_message"],
                    sent_at=datetime.now(timezone.utc),
                )
            )
            if outcome["status"] == "SENT":
                any_success = True
        if any_success:
            alerted_anomaly_ids.add(row["anomaly_id"])

    if digest_candidates:
        outcome = _send_email_digest(cfg, digest_candidates)
        sent_at = datetime.now(timezone.utc)
        for row in digest_candidates:
            alert_records.append(
                dict(
                    alert_id=str(uuid.uuid4()),
                    anomaly_id=row["anomaly_id"],
                    channel="email",
                    status=outcome["status"],
                    error_message=outcome["error_message"],
                    sent_at=sent_at,
                )
            )
            if outcome["status"] == "SENT":
                alerted_anomaly_ids.add(row["anomaly_id"])

    alert_log_df = spark.createDataFrame(alert_records, ALERT_LOG_SCHEMA)
    append_to_table(alert_log_df, cfg.state_table_full_name("alert_log"))

    if alerted_anomaly_ids:
        _mark_anomalies_alerted(cfg, list(alerted_anomaly_ids))

    logger.info(
        "Alerting run complete: %d anomaly(ies) processed, %d channel dispatch(es), %d marked alerted",
        len(rows),
        len(alert_records),
        len(alerted_anomaly_ids),
    )
    return alert_log_df


def _mark_anomalies_alerted(cfg: AppConfig, anomaly_ids: List[str]) -> None:
    spark = get_spark()
    updates = spark.createDataFrame([(aid, True) for aid in anomaly_ids], ["anomaly_id", "alerted"])
    merge_upsert(
        updates,
        cfg.state_table_full_name("anomaly_detection_log"),
        merge_keys=["anomaly_id"],
        update_columns=["alerted"],
        insert_when_not_matched=False,
    )


def retire_stale_config_rows(cfg: AppConfig) -> int:
    """Self-heal step: disables rules in anomaly_rules_config for tables that
    no longer exist in the catalog, so a dropped/renamed table doesn't keep
    generating alerts about itself being missing forever."""
    from .utils import table_exists

    spark = get_spark()
    rules_table = cfg.state_table_full_name("anomaly_rules_config")
    if not table_exists(rules_table):
        return 0

    rules = read_table(rules_table)
    distinct_tables = [r["table_name"] for r in rules.select("table_name").distinct().collect()]
    stale_tables = [t for t in distinct_tables if not table_exists(t)]
    if not stale_tables:
        return 0

    spark.sql(
        f"UPDATE {rules_table} SET enabled = false WHERE table_name IN "
        f"({', '.join([chr(39) + t + chr(39) for t in stale_tables])})"
    )
    logger.info("Retired config rows for missing table(s): %s", stale_tables)
    return len(stale_tables)
