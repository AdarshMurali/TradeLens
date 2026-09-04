"""OPTIONAL STRETCH: Structured Streaming variant (clearly a bonus module).

Demonstrates the OTHER kind of window: event-time windows + watermarking on a
streaming source. Reuses the same domain/schema as the batch pipeline.

Approach (free): file-source streaming — point readStream at a directory in S3
(or local) and drop new order files into it; process with maxFilesPerTrigger.
Alternatively use Amazon Kinesis (free tier) as the source.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F

from tradelens.common.config import load_config
from tradelens.common.logging_utils import get_logger
from tradelens.common.spark_session import get_spark

logger = get_logger(__name__)


def main() -> None:
    cfg = load_config()
    spark = get_spark("tradelens-streaming")
    raw = cfg.path("raw")

    # TODO: define an explicit schema (readStream cannot infer for file source).
    stream = (
        spark.readStream.schema("account_id string, symbol string, event_type string, "
                                "side string, price double, quantity long, event_time timestamp")
        .option("maxFilesPerTrigger", 1)
        .parquet(f"{raw}/orders_stream")
    )

    # Event-time windowed order counts with watermark for late trade corrections.
    agg = (
        stream.withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "1 minute", "30 seconds"), "symbol")
        .count()
    )

    query = (agg.writeStream.outputMode("update").format("console").start())
    query.awaitTermination()


if __name__ == "__main__":
    os.environ.setdefault("TRADELENS_ENV", "local")
    main()
