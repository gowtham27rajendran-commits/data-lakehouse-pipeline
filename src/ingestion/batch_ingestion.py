"""
Batch ingestion job: Raw source → Bronze Delta Lake layer.

Design decisions:
1. Write to Bronze as-is (no transforms) — enables full replay and debugging
2. Partition by ingestion_date for efficient time-range queries
3. Schema merge enabled for backward-compatible schema evolution
4. Idempotent writes using replaceWhere on partition to avoid duplicates on reruns

Run:
    spark-submit src/ingestion/batch_ingestion.py \
        --source s3a://raw/transactions/dt=2024-01-15/ \
        --target s3a://bronze/transactions/ \
        --partition-date 2024-01-15
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta import configure_spark_with_delta_pip
from config.spark_config import get_spark_session

logger = logging.getLogger(__name__)


def build_bronze_writer(spark: SparkSession, df: DataFrame, target_path: str, partition_date: str):
    """
    Write to Bronze layer with idempotency guarantee.
    
    Uses replaceWhere to overwrite only the target partition on rerun —
    this makes the job safe to re-execute on failure without duplicates.
    """
    return (
        df
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("ingestion_date", F.lit(partition_date))
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{partition_date}'")
        .option("mergeSchema", "true")         # Allow schema evolution
        .partitionBy("ingestion_date")
        .save(target_path)
    )


def read_source(spark: SparkSession, source_path: str, schema: StructType = None) -> DataFrame:
    """
    Read raw source files. Supports JSON, Parquet, CSV.
    Infers schema if not provided (Bronze layer — permissive).
    """
    reader = spark.read.option("recursiveFileLookup", "true")

    if source_path.endswith(".json") or "/json/" in source_path:
        reader = reader.option("mode", "PERMISSIVE")  # Don't fail on malformed rows
        if schema:
            return reader.schema(schema).json(source_path)
        return reader.json(source_path)

    elif source_path.endswith(".parquet") or "/parquet/" in source_path:
        return reader.parquet(source_path)

    elif source_path.endswith(".csv") or "/csv/" in source_path:
        return reader.option("header", "true").option("inferSchema", "true").csv(source_path)

    else:
        raise ValueError(f"Unsupported source format: {source_path}")


def add_data_quality_flags(df: DataFrame) -> DataFrame:
    """
    Add DQ flags to Bronze records for downstream Silver layer filtering.
    Does NOT drop rows — Bronze preserves all data, even bad rows.
    """
    return df.withColumns({
        "_dq_has_event_id": F.col("event_id").isNotNull() & (F.length(F.col("event_id")) > 0),
        "_dq_has_timestamp": F.col("timestamp").isNotNull() & (F.col("timestamp") > 0),
        "_dq_amount_positive": F.when(F.col("amount").isNotNull(), F.col("amount") >= 0).otherwise(True),
        "_dq_row_hash": F.md5(F.concat_ws("|", F.col("event_id"), F.col("timestamp"))),
    })


def run_batch_ingestion(
    source_path: str,
    target_path: str,
    partition_date: str,
    spark: SparkSession = None,
):
    if spark is None:
        spark = get_spark_session("bronze-batch-ingestion")

    logger.info(f"Starting batch ingestion: {source_path} → {target_path} (date={partition_date})")

    # Read
    raw_df = read_source(spark, source_path)
    logger.info(f"Read {raw_df.count()} records from source")

    # Add DQ flags (non-destructive at Bronze layer)
    flagged_df = add_data_quality_flags(raw_df)

    # Write to Bronze
    build_bronze_writer(spark, flagged_df, target_path, partition_date)

    logger.info(f"Bronze write complete: {target_path}/ingestion_date={partition_date}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="Source path (S3/local)")
    parser.add_argument("--target", required=True, help="Target Bronze Delta path")
    parser.add_argument("--partition-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    run_batch_ingestion(args.source, args.target, args.partition_date)
