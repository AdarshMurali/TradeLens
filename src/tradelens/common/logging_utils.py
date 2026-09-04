"""Centralized logging. Use get_logger(__name__) everywhere — never bare print()."""
from __future__ import annotations

import logging
import sys

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    global _CONFIGURED
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        root.addHandler(handler)
        # Spark is very chatty; quiet it down.
        logging.getLogger("py4j").setLevel(logging.WARNING)
        _CONFIGURED = True
    return logging.getLogger(name)
