"""Unit tests for the bronze layer: schema enforcement + lineage columns."""


def test_to_bronze_market_enforces_schema_and_adds_lineage(spark):
    from tradelens.bronze.ingest_to_bronze import to_bronze_market

    raw = spark.createDataFrame(
        [("100.0", "105.0", "99.0", "102.5", "1000", "AAPL", "2023-01-03")],
        ["open", "high", "low", "close", "volume", "symbol", "dt"],
    )
    out = to_bronze_market(raw)
    schema = {f.name: f.dataType.typeName() for f in out.schema.fields}

    assert schema["open"] == "double"
    assert schema["high"] == "double"
    assert schema["low"] == "double"
    assert schema["close"] == "double"
    assert schema["volume"] == "long"
    assert schema["dt"] == "date"
    assert "_ingested_at" in schema and "_source_file" in schema
    assert out.count() == raw.count()


def test_to_bronze_orders_enforces_schema_and_adds_lineage(spark):
    from tradelens.bronze.ingest_to_bronze import to_bronze_orders

    raw = spark.createDataFrame(
        [
            ("E1", "O1", "ACC1", "AAPL", "NEW", "BUY", "101.5", "500",
             "2023-01-03T10:00:00", "2023-01-03", "O0000001"),
            ("E2", "O2", "ACC1", "AAPL", "CANCEL", "SELL", "99.0", "10000",
             "2023-01-03T10:00:00.2", "2023-01-03", None),
        ],
        ["event_id", "order_id", "account_id", "symbol", "event_type", "side",
         "price", "quantity", "event_time", "dt", "parent_order_id"],
    )
    out = to_bronze_orders(raw)
    schema = {f.name: f.dataType.typeName() for f in out.schema.fields}

    assert schema["price"] == "double"
    assert schema["quantity"] == "long"
    assert schema["event_time"] == "timestamp"
    assert schema["parent_order_id"] == "string"
    assert schema["dt"] == "date"
    assert "_ingested_at" in schema and "_source_file" in schema
    assert out.count() == raw.count()
