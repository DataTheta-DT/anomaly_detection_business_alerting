"""
config.py
---------
Loads and resolves the project's single JSON configuration file.

No other module in this package should read environment-specific values
(catalog names, table names, thresholds, email addresses, etc.) directly.
Everything flows through the AppConfig object returned by load_config().
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

DEFAULT_CONFIG_PATH = os.environ.get(
    "ANOMALY_ALERTING_CONFIG_PATH",
    "/Workspace/anomaly_detection_business_alerting/config/config.json",
)


class ConfigError(Exception):
    """Raised when the configuration file is missing, malformed, or invalid."""


def _resolve_placeholders(value: Any, context: Dict[str, str]) -> Any:
    """Recursively substitutes ``{key}`` placeholders in strings using ``context``."""
    if isinstance(value, str):
        try:
            return value.format(**context)
        except (KeyError, IndexError):
            return value
    if isinstance(value, dict):
        return {k: _resolve_placeholders(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_placeholders(v, context) for v in value]
    return value


@dataclass
class AppConfig:
    """Typed, resolved view over config.json. Access nested config via .raw when needed."""

    raw: Dict[str, Any]

    # ---------- Unity Catalog ----------
    @property
    def catalog(self) -> str:
        return self.raw["unity_catalog"]["catalog"]

    @property
    def source_schema(self) -> str:
        return self.raw["unity_catalog"]["source_schema"]

    @property
    def config_schema(self) -> str:
        return self.raw["unity_catalog"]["config_schema"]

    @property
    def source_table_full_name(self) -> str:
        return self.raw["unity_catalog"]["source_table_full_name"]

    def state_table_full_name(self, logical_name: str) -> str:
        """Returns the fully qualified name for a framework state table.

        logical_name is one of the keys under `state_tables` in config.json,
        e.g. 'metrics_baseline', 'anomaly_rules_config'.
        """
        table = self.raw["state_tables"][logical_name]
        return f"{self.catalog}.{self.config_schema}.{table}"

    # ---------- Source data ----------
    @property
    def source_data(self) -> Dict[str, Any]:
        return self.raw["source_data"]

    # ---------- Metrics engine ----------
    @property
    def metrics_engine(self) -> Dict[str, Any]:
        return self.raw["metrics_engine"]

    # ---------- Anomaly rules ----------
    @property
    def anomaly_rules(self) -> Dict[str, Any]:
        return self.raw["anomaly_rules"]

    def rule(self, anomaly_type: str) -> Dict[str, Any]:
        rules = self.anomaly_rules
        if anomaly_type not in rules:
            raise ConfigError(f"No rule configured for anomaly type '{anomaly_type}'")
        return rules[anomaly_type]

    def enabled_anomaly_types(self) -> List[str]:
        return [name for name, cfg in self.anomaly_rules.items() if cfg.get("enabled", False)]

    # ---------- Alerting ----------
    @property
    def severity_routing(self) -> Dict[str, List[str]]:
        return self.raw["severity_routing"]

    @property
    def alerting(self) -> Dict[str, Any]:
        return self.raw["alerting"]

    def channels_for_severity(self, severity: str) -> List[str]:
        return self.severity_routing.get(severity, [])

    # ---------- Logging ----------
    @property
    def logging_cfg(self) -> Dict[str, Any]:
        return self.raw["logging"]

    @property
    def log_level(self) -> str:
        return self.raw.get("environment", {}).get("log_level", "INFO")

    @property
    def environment_name(self) -> str:
        return self.raw.get("environment", {}).get("name", "dev")


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """Loads config.json from disk, resolves {catalog}/{schema}-style placeholders,
    and returns an AppConfig object.

    Parameters
    ----------
    config_path:
        Optional override path. Defaults to DEFAULT_CONFIG_PATH, which itself can be
        overridden with the ANOMALY_ALERTING_CONFIG_PATH environment variable. This
        lets the same code run unmodified across dev/staging/prod workspaces -- only
        the config file (or the env var pointing at it) changes.
    """
    path = config_path or DEFAULT_CONFIG_PATH
    if not os.path.exists(path):
        raise ConfigError(
            f"Configuration file not found at '{path}'. "
            "Set ANOMALY_ALERTING_CONFIG_PATH or pass config_path explicitly."
        )

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    context = {
        "catalog": raw["unity_catalog"]["catalog"],
        "source_schema": raw["unity_catalog"]["source_schema"],
        "config_schema": raw["unity_catalog"]["config_schema"],
        "source_table": raw["unity_catalog"]["source_table"],
    }
    raw["unity_catalog"]["source_table_full_name"] = _resolve_placeholders(
        raw["unity_catalog"]["source_table_full_name"], context
    )
    raw["logging"]["log_table"] = _resolve_placeholders(raw["logging"]["log_table"], context)

    _validate(raw)
    return AppConfig(raw=raw)


def _validate(raw: Dict[str, Any]) -> None:
    """Fails fast on obviously broken configuration rather than surfacing a
    cryptic error deep inside a Spark job."""
    required_top_level = [
        "unity_catalog",
        "state_tables",
        "source_data",
        "metrics_engine",
        "anomaly_rules",
        "severity_routing",
        "alerting",
        "logging",
    ]
    missing = [key for key in required_top_level if key not in raw]
    if missing:
        raise ConfigError(f"config.json is missing required section(s): {missing}")

    if not raw["unity_catalog"].get("catalog"):
        raise ConfigError("unity_catalog.catalog must be set")

    for anomaly_type, rule_cfg in raw["anomaly_rules"].items():
        if "severity" not in rule_cfg:
            raise ConfigError(f"anomaly_rules.{anomaly_type} is missing 'severity'")
        if rule_cfg["severity"] not in raw["severity_routing"]:
            raise ConfigError(
                f"anomaly_rules.{anomaly_type} severity '{rule_cfg['severity']}' "
                f"has no entry in severity_routing"
            )
