"""Synthetic order-event generator.

Produces order lifecycle events (NEW / MODIFY / CANCEL / FILL) layered on top of
the real OHLCV, and DELIBERATELY INJECTS market-abuse patterns so the surveillance
layer has something to detect:

  * spoofing  - large order placed on one side then cancelled within ms, near a
                genuine fill on the opposite side.
  * wash trade- same beneficial owner (account) on both buy and sell, no real
                economic transfer.
  * layering  - several orders stacked at successive price levels then pulled.

A ground-truth label file is written alongside so detection precision/recall can
be measured later. Always disclose in interviews that this data is synthetic.

Run: python -m tradelens.ingestion.generate_order_events
Output: <raw>/orders/<symbol>.parquet         -- one file per symbol
        <raw>/orders_labels/<symbol>.parquet  -- one file per symbol (ground truth)
Not one-file-per-symbol-day: see fetch_market_data.py's docstring for why.

NOTE: This is intentionally a clear, extensible skeleton. Flesh out the injection
functions (see TODOs) to make patterns more realistic. The event schema and the
labeling contract below are the important, stable parts.
"""
from __future__ import annotations

import os
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from tradelens.common.config import load_config
from tradelens.common.logging_utils import get_logger

logger = get_logger(__name__)

EVENT_SCHEMA = [
    "event_id", "order_id", "account_id", "symbol", "event_type", "side",
    "price", "quantity", "event_time", "dt", "parent_order_id",
]
# event_type in {NEW, MODIFY, CANCEL, FILL}; side in {BUY, SELL}

# A config `*_rate` is "fraction of orders that are this abuse pattern"; this
# converts that into a per-symbol-day probability of injecting one episode
# (an episode contributes several orders, not one, so the rate alone would
# under-inject relative to `orders_per_symbol_per_day`).
EPISODES_PER_RATE_UNIT = 50


@dataclass
class OrderEvent:
    event_id: str
    order_id: str
    account_id: str
    symbol: str
    event_type: str
    side: str
    price: float
    quantity: int
    event_time: str
    dt: str
    parent_order_id: str | None = None


def _load_reference_prices(raw_root: str, symbol: str) -> pd.DataFrame:
    """Read the landed OHLCV for a symbol to anchor realistic prices."""
    f = Path(raw_root) / "market" / f"{symbol}.parquet"
    if not f.exists():
        return pd.DataFrame()
    return pd.read_parquet(f)


def _gen_normal_orders(symbol, day, prices_row, accounts, n, rng, counter):
    """Generate ordinary, non-abusive order lifecycles for one symbol-day."""
    events: list[OrderEvent] = []
    labels: list[dict] = []
    base = float(prices_row["close"])
    day_start = datetime.fromisoformat(str(day)) + timedelta(hours=9, minutes=30)
    for _ in range(n):
        counter["v"] += 1
        oid = f"O{counter['v']:09d}"
        acct = rng.choice(accounts)
        side = rng.choice(["BUY", "SELL"])
        px = round(base * (1 + rng.uniform(-0.01, 0.01)), 2)
        qty = rng.choice([100, 200, 500, 1000])
        t = day_start + timedelta(seconds=rng.randint(0, 6 * 3600))
        events.append(OrderEvent(f"E{counter['v']}N", oid, acct, symbol, "NEW", side, px, qty,
                                 t.isoformat(), str(day)))
        # most fill, some cancel normally
        if rng.random() < 0.7:
            tf = t + timedelta(seconds=rng.randint(1, 300))
            events.append(OrderEvent(f"E{counter['v']}F", oid, acct, symbol, "FILL", side, px, qty,
                                     tf.isoformat(), str(day)))
        else:
            tc = t + timedelta(seconds=rng.randint(1, 300))
            events.append(OrderEvent(f"E{counter['v']}C", oid, acct, symbol, "CANCEL", side, px, qty,
                                     tc.isoformat(), str(day)))
        labels.append({"order_id": oid, "label": "normal", "pattern": None})
    return events, labels


def _inject_spoofing(symbol, day, prices_row, accounts, rng, counter):
    """TODO: refine. A large order placed then cancelled within `spoof_cancel_ms`,
    opposite side of a genuine fill. Returns (events, labels)."""
    events, labels = [], []
    base = float(prices_row["close"])
    day_start = datetime.fromisoformat(str(day)) + timedelta(hours=10)
    counter["v"] += 1
    oid = f"O{counter['v']:09d}"
    acct = rng.choice(accounts)
    side = "SELL"  # spoof side
    px = round(base * 1.002, 2)
    qty = rng.choice([5000, 10000])  # unusually large
    t = day_start + timedelta(seconds=rng.randint(0, 3600))
    events.append(OrderEvent(f"E{counter['v']}N", oid, acct, symbol, "NEW", side, px, qty,
                             t.isoformat(), str(day)))
    tc = t + timedelta(milliseconds=rng.randint(50, 400))  # cancelled very fast
    events.append(OrderEvent(f"E{counter['v']}C", oid, acct, symbol, "CANCEL", side, px, qty,
                             tc.isoformat(), str(day)))
    # genuine opposite-side fill by same account shortly after
    counter["v"] += 1
    oid2 = f"O{counter['v']:09d}"
    events.append(OrderEvent(f"E{counter['v']}N", oid2, acct, symbol, "NEW", "BUY", round(base, 2),
                             500, (t + timedelta(seconds=1)).isoformat(), str(day)))
    events.append(OrderEvent(f"E{counter['v']}F", oid2, acct, symbol, "FILL", "BUY", round(base, 2),
                             500, (t + timedelta(seconds=2)).isoformat(), str(day)))
    labels.append({"order_id": oid, "label": "abuse", "pattern": "spoofing"})
    return events, labels


def _inject_wash_trade(symbol, day, prices_row, accounts, rng, counter):
    """TODO: refine. Same account both sides at same price/time -> no economic change."""
    events, labels = [], []
    base = float(prices_row["close"])
    t = datetime.fromisoformat(str(day)) + timedelta(hours=11, seconds=rng.randint(0, 3600))
    acct = rng.choice(accounts)
    px, qty = round(base, 2), 1000
    for sd in ("BUY", "SELL"):
        counter["v"] += 1
        oid = f"O{counter['v']:09d}"
        events.append(OrderEvent(f"E{counter['v']}N", oid, acct, symbol, "NEW", sd, px, qty,
                                 t.isoformat(), str(day)))
        events.append(OrderEvent(f"E{counter['v']}F", oid, acct, symbol, "FILL", sd, px, qty,
                                 (t + timedelta(seconds=1)).isoformat(), str(day)))
        labels.append({"order_id": oid, "label": "abuse", "pattern": "wash_trade"})
    return events, labels


def _inject_layering(symbol, day, prices_row, accounts, rng, counter):
    """Several orders stacked at successive price levels away from the touch
    on one side, then all pulled together shortly after — creates a false
    impression of book depth without any of them being meant to fill."""
    events, labels = [], []
    base = float(prices_row["close"])
    t0 = datetime.fromisoformat(str(day)) + timedelta(hours=13, seconds=rng.randint(0, 3600))
    acct = rng.choice(accounts)
    side = rng.choice(["BUY", "SELL"])
    direction = 1 if side == "SELL" else -1  # stack away from mid price
    n_layers = rng.randint(4, 6)

    layers = []  # (order_id, price, quantity)
    for i in range(1, n_layers + 1):
        counter["v"] += 1
        oid = f"O{counter['v']:09d}"
        px = round(base * (1 + direction * 0.001 * i), 2)
        qty = rng.choice([1000, 1500, 2000])
        t = t0 + timedelta(milliseconds=rng.randint(0, 300) * i)
        events.append(OrderEvent(f"E{counter['v']}N", oid, acct, symbol, "NEW", side, px, qty,
                                 t.isoformat(), str(day)))
        labels.append({"order_id": oid, "label": "abuse", "pattern": "layering"})
        layers.append((oid, px, qty))

    # Pull the whole stack shortly after, before any could realistically fill.
    # Anchored after the *last possible* NEW timestamp (milliseconds(300 * n_layers)
    # above), not just after t0 — otherwise a higher-numbered layer's NEW jitter
    # can land after an early cancel_base draw, putting its CANCEL before its own
    # NEW in event-time order.
    last_new_offset = timedelta(milliseconds=300 * n_layers)
    cancel_base = t0 + last_new_offset + timedelta(seconds=rng.randint(1, 5))
    for i, (oid, px, qty) in enumerate(layers, start=1):
        counter["v"] += 1
        tc = cancel_base + timedelta(milliseconds=rng.randint(0, 200) * i)
        events.append(OrderEvent(f"E{counter['v']}C", oid, acct, symbol, "CANCEL", side, px, qty,
                                 tc.isoformat(), str(day)))
    return events, labels


def generate() -> None:
    cfg = load_config()
    raw_root = cfg.path("raw")
    syn = cfg["synthetic"]
    rng = random.Random(syn["seed"])
    accounts = [f"ACC{i:04d}" for i in range(syn["accounts"])]
    counter = {"v": 0}

    for symbol in cfg["symbols"]:
        ref = _load_reference_prices(raw_root, symbol)
        if ref.empty:
            logger.warning("No reference prices for %s; run fetch_market_data first.", symbol)
            continue

        # Accumulate every day's events/labels for this symbol and write ONCE
        # at the end, rather than once per day -- one-file-per-symbol-day was
        # the exact small-file pattern already fixed on the write side (see
        # module docstring); this is the same fix applied to generation.
        symbol_events: list[OrderEvent] = []
        symbol_labels: list[dict] = []
        for _, row in ref.iterrows():
            day = row["dt"]
            ev, lb = _gen_normal_orders(symbol, day, row, accounts,
                                        syn["orders_per_symbol_per_day"], rng, counter)
            if rng.random() < syn["inject"]["spoofing_rate"] * EPISODES_PER_RATE_UNIT:
                e, l = _inject_spoofing(symbol, day, row, accounts, rng, counter); ev += e; lb += l
            if rng.random() < syn["inject"]["wash_trade_rate"] * EPISODES_PER_RATE_UNIT:
                e, l = _inject_wash_trade(symbol, day, row, accounts, rng, counter); ev += e; lb += l
            if rng.random() < syn["inject"]["layering_rate"] * EPISODES_PER_RATE_UNIT:
                e, l = _inject_layering(symbol, day, row, accounts, rng, counter); ev += e; lb += l
            symbol_events += ev
            symbol_labels += lb

        out = Path(raw_root) / "orders"; out.mkdir(parents=True, exist_ok=True)
        ev_df = pd.DataFrame([asdict(x) for x in symbol_events])[EVENT_SCHEMA]
        ev_df.to_parquet(out / f"{symbol}.parquet", index=False)

        lout = Path(raw_root) / "orders_labels"; lout.mkdir(parents=True, exist_ok=True)
        lb_df = pd.DataFrame(symbol_labels)
        lb_df.to_parquet(lout / f"{symbol}.parquet", index=False)

        logger.info("Generated %d order events for %s", len(ev_df), symbol)

    logger.info("Order events + ground-truth labels landed under %s", raw_root)


if __name__ == "__main__":
    os.environ.setdefault("TRADELENS_ENV", "local")
    generate()
