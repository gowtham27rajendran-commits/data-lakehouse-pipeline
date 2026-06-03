"""
Streaming ingestion: Kafka → Bronze Delta Lake using Spark Structured Streaming.

Key design decisions:

1. WATERMARKING for late-arriving data:
   - Retail events can arrive late due to network issues at store POS terminals
   - Watermark of 2 hours: Spark will hold state for 2h past max event-time seen
   - Events arriving > 2h late are still written but may miss windowed aggregations

2. CHECKPOINTING:
   - WAL-based checkpoint to S3 ensures exactly-once processing on restart
   - Checkpoint location must be unique per streaming query

3. TRIGGER:
   - processingTime="30 seconds" balances latency vs overhead
   - For lower latency needs, use "1 second" or "availableNow" for burst

4. OUTPUT MODE:
   - "append" for Bronze (never update raw records)
   - "update" used in Silver aggregation streams

Run:
    spark-submit --packages io.delta:delta-core_2.12:3.0.0,\
        org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        src/ingestion/streaming_ingestion.py \
        --topic transactions \
        --bootstrap-servers localhost:9092 \
        --checkpoint s3a://checkpoints/transactions/
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from config.spark_config import get_spark_session
from config.schemas import TRANSACTION_SCHEMA

logger = logging.getLogger(__name__)

BRONZE_BASE_PATH = "s3a://bronze"
WATERMARK_DELAY = "2 hours"


def parse_kafka_message(df, schema: StructType):
    """
    Parse Kafka message value (bytes) into structured columns.
    Kafka message: topic, partition, offset, timestamp, key, value (bytes)
    """
    return (
        df
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(
            F.from_json(F.col("json_str"), schema).alias("data"),
            F.col("kafka_ts"),
        )
        .select("data.*", "kafka_ts")
    )


def apply_watermark(df, event_time_col: str = "timestamp"):
    """
    Apply watermark for late-arriving event handling.
    Converts ms epoch to timestamp type for Spark's event-time processing.
    """
    return (
        df
        .withColumn("event_time", (F.col(event_time_col) / 1000).cast("timestamp"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )


def run_streaming_ingestion(
    topic: str,
    bootstrap_servers: str,
    checkpoint_path: str,
    spark: SparkSession = None,
):
    if spark is None:
        spark = get_spark_session("streaming-ingestion")

    target_path = f"{BRONZE_BASE_PATH}/{topic}"

    logger.info(f"Starting streaming ingestion: kafka://{topic} → {target_path}")

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")     # Don't fail if Kafka log is compacted
        .option("maxOffsetsPerTrigger", 100000) # Backpressure: max 100k events per micro-batch
        .load()
    )

    # Parse + watermark
    schema = TRANSACTION_SCHEMA  # Topic-specific schema lookup would go here
    parsed_stream = parse_kafka_message(raw_stream, schema)
    watermarked = apply_watermark(parsed_stream)

    # Add ingestion metadata
    enriched = watermarked.withColumns({
        "_ingestion_ts": F.current_timestamp(),
        "_topic": F.lit(topic),
        "ingestion_date": F.to_date(F.col("event_time")),
    })

    # Write to Bronze Delta with checkpointing
    query = (
        enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("ingestion_date")
        .trigger(processingTime="30 seconds")
        .start(target_path)
    )

    logger.info(f"Streaming query started: {query.id}")
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint path (S3/HDFS/local)")
    args = parser.parse_args()

    run_streaming_ingestion(args.topic, args.bootstrap_servers, args.checkpoint)
