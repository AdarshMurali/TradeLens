"""Load config.yaml + .env and resolve storage paths for the active environment.

Usage:
    from tradelens.common.config import load_config
    cfg = load_config()
    gold_path = cfg.path("gold")   # local dir or s3a:// depending on TRADELENS_ENV
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env(value: Any) -> Any:
    """Recursively replace ${VAR} with environment values."""
    if isinstance(value, str):
        return _ENV_PATTERN.sub(lambda m: os.getenv(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


@dataclass
class Config:
    raw: dict
    env: str  # "local" or "aws"

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.raw.get(key, default)

    def path(self, layer: str) -> str:
        """Return the storage root for a layer (raw/bronze/silver/gold)."""
        return self.raw["paths"][self.env][layer]


def load_config(config_path: str | Path = "config/config.yaml") -> Config:
    load_dotenv()  # loads .env if present
    env = os.getenv("TRADELENS_ENV", "local")
    # On EMR Serverless, spark-submit's --files stages config.yaml into the
    # job's flat working directory (no "config/" subdirectory survives), so
    # scripts/submit_emr_serverless.sh points this at the bare filename via
    # spark.emr-serverless.driverEnv.TRADELENS_CONFIG_PATH.
    config_path = os.getenv("TRADELENS_CONFIG_PATH", config_path)
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    raw = _expand_env(raw)
    return Config(raw=raw, env=env)
