"""Unit tests for the synthetic order-event generator's injection functions.

These are pure functions (inputs -> lists of OrderEvent/label dicts, no I/O),
so they're tested directly with plain Python — no Spark needed.
"""
import random

from tradelens.ingestion.generate_order_events import (
    EVENT_SCHEMA,
    OrderEvent,
    _gen_normal_orders,
    _inject_layering,
    _inject_spoofing,
    _inject_wash_trade,
)

SYMBOL = "AAPL"
DAY = "2023-01-03"
PRICE_ROW = {"close": 150.0}
ACCOUNTS = [f"ACC{i:04d}" for i in range(10)]


def test_event_schema_matches_order_event_fields():
    assert set(EVENT_SCHEMA) == set(OrderEvent.__dataclass_fields__.keys())


def test_gen_normal_orders_each_order_has_new_and_terminal_event():
    rng = random.Random(1)
    events, labels = _gen_normal_orders(SYMBOL, DAY, PRICE_ROW, ACCOUNTS, 20, rng, {"v": 0})

    by_order = {}
    for e in events:
        by_order.setdefault(e.order_id, []).append(e.event_type)

    assert len(by_order) == 20
    for order_id, types in by_order.items():
        assert types[0] == "NEW"
        assert set(types[1:]) <= {"FILL", "CANCEL"}
        assert len(types) == 2

    assert all(l["label"] == "normal" and l["pattern"] is None for l in labels)


def test_inject_spoofing_flags_large_order_cancelled_fast():
    rng = random.Random(2)
    events, labels = _inject_spoofing(SYMBOL, DAY, PRICE_ROW, ACCOUNTS, rng, {"v": 0})

    spoof_labels = [l for l in labels if l["pattern"] == "spoofing"]
    assert len(spoof_labels) == 1
    spoof_order_id = spoof_labels[0]["order_id"]
    assert spoof_labels[0]["label"] == "abuse"

    spoof_events = [e for e in events if e.order_id == spoof_order_id]
    assert [e.event_type for e in spoof_events] == ["NEW", "CANCEL"]
    assert spoof_events[0].quantity >= 5000  # unusually large

    new_evt, cancel_evt = spoof_events
    from datetime import datetime

    delta_ms = (
        datetime.fromisoformat(cancel_evt.event_time) - datetime.fromisoformat(new_evt.event_time)
    ).total_seconds() * 1000
    assert 0 < delta_ms < 500  # cancelled fast, per surveillance.spoof_cancel_ms

    # a genuine opposite-side fill by the same account follows
    other_order_events = [e for e in events if e.order_id != spoof_order_id]
    assert any(e.event_type == "FILL" and e.side == "BUY" for e in other_order_events)
    assert all(e.account_id == spoof_events[0].account_id for e in other_order_events)


def test_inject_wash_trade_same_account_both_sides_no_price_change():
    rng = random.Random(3)
    events, labels = _inject_wash_trade(SYMBOL, DAY, PRICE_ROW, ACCOUNTS, rng, {"v": 0})

    assert len(labels) == 2
    assert all(l["label"] == "abuse" and l["pattern"] == "wash_trade" for l in labels)

    accounts_used = {e.account_id for e in events}
    assert len(accounts_used) == 1  # same beneficial owner both sides

    sides = {e.side for e in events if e.event_type == "NEW"}
    assert sides == {"BUY", "SELL"}

    prices = {e.price for e in events}
    assert len(prices) == 1  # no economic price change


def test_inject_layering_stacks_orders_then_cancels_all():
    rng = random.Random(4)
    events, labels = _inject_layering(SYMBOL, DAY, PRICE_ROW, ACCOUNTS, rng, {"v": 0})

    assert 4 <= len(labels) <= 6
    assert all(l["label"] == "abuse" and l["pattern"] == "layering" for l in labels)

    order_ids = {l["order_id"] for l in labels}
    assert len(order_ids) == len(labels)

    by_order = {}
    for e in events:
        by_order.setdefault(e.order_id, []).append(e.event_type)

    assert set(by_order.keys()) == order_ids
    for types in by_order.values():
        # every layer is placed then pulled — never filled
        assert types == ["NEW", "CANCEL"]

    accounts_used = {e.account_id for e in events}
    sides_used = {e.side for e in events}
    assert len(accounts_used) == 1
    assert len(sides_used) == 1  # all layers stacked on the same side

    # prices are distinct successive levels away from the reference price
    new_prices = sorted(e.price for e in events if e.event_type == "NEW")
    assert len(new_prices) == len(set(new_prices))
