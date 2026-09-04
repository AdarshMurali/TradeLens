"""Fetch historical OHLCV via yfinance and land it as raw partitioned files.

Free, no API key. Run: python -m tradelens.ingestion.fetch_market_data
Output: <raw>/market/dt=<date>/<symbol>.parquet   (partition-friendly)
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from tradelens.common.config import load_config
from tradelens.common.logging_utils import get_logger

logger = get_logger(__name__)


def fetch() -> None:
    import yfinance as yf  # imported here so the module loads even without the dep

    cfg = load_config()
    symbols = cfg["symbols"]
    dr = cfg["date_range"]
    raw_root = cfg.path("raw")

    logger.info("Fetching %d symbols %s..%s @ %s", len(symbols), dr["start"], dr["end"], dr["interval"])
    for symbol in symbols:
        df = yf.download(
            symbol, start=dr["start"], end=dr["end"], interval=dr["interval"], progress=False
        )
        if df.empty:
            logger.warning("No data for %s", symbol)
            continue
        df = df.reset_index()
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
        df["symbol"] = symbol
        date_col = "date" if "date" in df.columns else "datetime"
        df["dt"] = pd.to_datetime(df[date_col]).dt.date.astype(str)

        # Write one file per (symbol) partitioned by dt via a single directory tree.
        for dt, part in df.groupby("dt"):
            out_dir = Path(raw_root) / "market" / f"dt={dt}"
            out_dir.mkdir(parents=True, exist_ok=True)
            part.to_parquet(out_dir / f"{symbol}.parquet", index=False)
        logger.info("Wrote %s rows for %s", len(df), symbol)

    logger.info("Market data landed under %s/market", raw_root)


if __name__ == "__main__":
    os.environ.setdefault("TRADELENS_ENV", "local")
    fetch()
