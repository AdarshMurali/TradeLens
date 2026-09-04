"""Tests the Delta MERGE INTO + time-travel mechanics used by
jobs/apply_trade_corrections.py, against a real local Delta table (MERGE is
a stateful table operation — there's no pure in-memory equivalent to test).
"""
from delta.tables import DeltaTable


def test_merge_into_corrections_updates_and_time_travel_shows_prior_state(spark, tmp_path):
    fills_path = str(tmp_path / "fills")

    original = spark.createDataFrame(
        [("E1", 1000, False), ("E2", 500, False)],
        ["event_id", "quantity", "is_busted"],
    )
    original.write.format("delta").save(fills_path)
    pre_merge_version = DeltaTable.forPath(spark, fills_path).history(1).collect()[0]["version"]

    corrections = spark.createDataFrame(
        [("E1", 1000, True), ("E2", 450, False)],  # E1 busted, E2 quantity amended
        ["event_id", "quantity", "is_busted"],
    )
    (
        DeltaTable.forPath(spark, fills_path)
        .alias("t")
        .merge(corrections.alias("c"), "t.event_id = c.event_id")
        .whenMatchedUpdate(set={"quantity": "c.quantity", "is_busted": "c.is_busted"})
        .execute()
    )

    current = {r["event_id"]: r for r in spark.read.format("delta").load(fills_path).collect()}
    assert current["E1"]["is_busted"] is True
    assert current["E2"]["quantity"] == 450

    # time travel: the pre-merge version still shows the original state
    before = {
        r["event_id"]: r
        for r in spark.read.format("delta").option("versionAsOf", pre_merge_version)
        .load(fills_path).collect()
    }
    assert before["E1"]["is_busted"] is False
    assert before["E2"]["quantity"] == 500
