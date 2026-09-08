"""Fetch historical OHLCV via yfinance and land it as raw files.

Free, no API key. Run: python -m tradelens.ingestion.fetch_market_data
Output: <raw>/market/<symbol>.parquet   -- one file per symbol, spanning the
whole date range. NOT one-file-per-symbol-day: `dt` is already a real column
in the schema (not just path-encoded), so a per-day directory layout bought
nothing downstream except a file-count explosion that scales with
symbols x days instead of symbols alone -- confirmed against real data,
see the small-file fix in jobs/run_pipeline.py's _write_delta for the
bronze/silver/gold-layer version of the same bug.
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
        if isinstance(df.columns, pd.MultiIndex):
            # yfinance returns (field, ticker) MultiIndex columns even for a
            # single symbol; drop the ticker level before flattening names.
            df.columns = df.columns.get_level_values(0)
        df = df.reset_index()
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
        df["symbol"] = symbol
        date_col = "date" if "date" in df.columns else "datetime"
        df["dt"] = pd.to_datetime(df[date_col]).dt.date.astype(str)
        # Drop the raw datetime64[ns] column: pyarrow writes it as a
        # nanosecond-precision Parquet timestamp, which Spark cannot read
        # ("Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))"). The
        # string `dt` column above is the documented date column downstream.
        df = df.drop(columns=[date_col])

        out_dir = Path(raw_root) / "market"
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_dir / f"{symbol}.parquet", index=False)
        logger.info("Wrote %s rows for %s", len(df), symbol)

    logger.info("Market data landed under %s/market", raw_root)


if __name__ == "__main__":
    os.environ.setdefault("TRADELENS_ENV", "local")
    fetch()
