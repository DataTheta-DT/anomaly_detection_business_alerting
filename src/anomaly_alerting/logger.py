"""
logger.py
---------
Central logging setup for the accelerator. All modules call get_logger(__name__)
instead of configuring logging themselves, so log level and format stay
consistent and are driven entirely by config.json (environment.log_level).
"""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False


def configure_logging(level: str = "INFO") -> None:
    """Configures the root logger once per process. Safe to call multiple times."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger()
    root.setLevel(level.upper())

    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    root.handlers = [handler]
    _CONFIGURED = True


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    configure_logging(level)
    return logging.getLogger(name)
