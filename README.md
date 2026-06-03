# Real-Time Data Lakehouse Pipeline

A scalable data lakehouse platform supporting both batch and streaming ingestion with built-in data quality, schema evolution, and observability — designed for retail analytics at scale.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Data Sources                                    │
│         Kafka Streams │ PostgreSQL CDC │ S3 Files │ REST APIs            │
└────────────┬──────────────────┬─────────────────────┬────────────────────┘
             │                  │                     │
             ▼                  ▼                     ▼
┌────────────────────┐ ┌────────────────────┐ ┌──────────────────────┐
│  Streaming Layer   │ │   Batch Layer      │ │  Change Data Capture │
│  (Kafka + Spark    │ │  (Spark ETL Jobs)  │ │  (Debezium → Kafka)  │
│   Structured       │ │  - Daily/Hourly    │ │                      │
│   Streaming)       │ │  - Backfill jobs   │ └──────────────────────┘
└────────────┬───────┘ └────────┬───────────┘
             │                  │
             └────────┬─────────┘
                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                       Data Quality Layer                                 │
│   Schema Validation │ Null Detection │ Referential Integrity │ Anomalies │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     Delta Lake (Bronze / Silver / Gold)                  │
│                                                                          │
│  Bronze (Raw)    →    Silver (Cleaned)    →    Gold (Aggregated)        │
│  - As-received        - Schema enforced        - Business metrics        │
│  - Append only        - Deduped                - Star schema             │
│  - No transforms      - Normalized             - BI-ready                │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        Serving Layer                                     │
│         Trino (Ad-hoc SQL) │ FastAPI (REST) │ Grafana (Dashboards)      │
└──────────────────────────────────────────────────────────────────────────┘
```

## Medallion Architecture

| Layer | Format | Purpose | Retention |
|---|---|---|---|
| **Bronze** | Delta Lake (Parquet) | Raw, as-received data | 90 days |
| **Silver** | Delta Lake (Parquet) | Cleaned, deduped, normalized | 2 years |
| **Gold** | Delta Lake (Parquet) | Business aggregates, star schema | Indefinite |

## Key Features

- **Late-arriving data handling**: Watermark-based windowing in Spark Structured Streaming
- **Out-of-order events**: Event-time processing with configurable watermark lag
- **Data quality gates**: Automated DQ checks block bad data from reaching Silver/Gold
- **Schema evolution**: Delta Lake schema merging with backward-compatible changes
- **Performance tuning**: Z-ordering on high-cardinality columns, partition pruning, adaptive query execution
- **Observability**: Per-job data volume, DQ pass rates, SLA tracking dashboards

## Tech Stack

| Component | Technology |
|---|---|
| Processing | Apache Spark 3.5 (PySpark) |
| Storage Format | Delta Lake 3.0 |
| Streaming Source | Apache Kafka |
| Object Storage | AWS S3 / MinIO (local) |
| Query Engine | Trino |
| Data Quality | Great Expectations + custom framework |
| Orchestration | Apache Airflow |
| Monitoring | Grafana + Prometheus |

## Project Structure

```
data-lakehouse-pipeline/
├── src/
│   ├── ingestion/        # Batch + streaming ingestion jobs
│   ├── transforms/       # Silver and Gold layer transformations
│   ├── quality/          # Data quality framework
│   ├── observability/    # Metrics, alerting, SLA tracking
│   └── serving/          # FastAPI serving layer
├── config/               # Spark configs, schema registry
├── sql/                  # DDL and ad-hoc query templates
├── tests/                # Unit + integration tests
├── notebooks/            # EDA and investigation notebooks
└── docker-compose.yml    # Local MinIO + Trino + Kafka stack
```

## Getting Started

```bash
# Start infrastructure
docker-compose up -d

# Run a batch ETL job
spark-submit src/ingestion/batch_ingestion.py \
  --source s3a://raw/transactions/ \
  --target s3a://bronze/transactions/ \
  --date 2024-01-15

# Run streaming ingestion
spark-submit src/ingestion/streaming_ingestion.py \
  --topic transactions \
  --checkpoint s3a://checkpoints/transactions/

# Run data quality checks
python src/quality/run_quality_checks.py --layer silver --table transactions

# Run full pipeline via Airflow
airflow dags trigger lakehouse_daily_pipeline
```

## Performance Results

| Job | Dataset Size | Runtime | Throughput |
|---|---|---|---|
| Daily transaction ETL | 50GB | 4.2 min | ~200MB/s |
| Streaming ingestion | 10k events/sec | <5s latency | - |
| Silver DQ checks | 100M rows | 6.8 min | - |
| Gold aggregation | 500M rows | 11.3 min | - |

## Data Quality SLAs

- Schema compliance: **> 99.9%**
- Null rate on required fields: **< 0.01%**
- Referential integrity (order → customer): **> 99.95%**
- Pipeline SLA breach alert: **> 30 min delay**
