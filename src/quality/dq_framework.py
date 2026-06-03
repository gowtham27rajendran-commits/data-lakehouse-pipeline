"""
Data quality framework for lakehouse pipeline.

Implements layered DQ checks for Silver and Gold layers:
1. Schema compliance (required fields, type correctness)
2. Null/completeness checks on critical columns
3. Referential integrity (e.g., order.customer_id exists in customers table)
4. Statistical anomaly detection (volume spikes, mean/stddev drift)
5. Freshness checks (data not older than SLA threshold)

Design:
- DQ checks run as Spark jobs — scalable to billions of rows
- Results are written to a DQ results Delta table for trending
- Failed checks emit Prometheus alerts, blocking Gold promotion if critical
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class DQCheckResult:
    check_name: str
    table: str
    layer: str
    passed: bool
    severity: str        # "critical" | "warning" | "info"
    metric_value: float
    threshold: float
    details: str = ""
    run_ts: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class DataQualityFramework:

    def __init__(self, spark: SparkSession, dq_results_path: str = "s3a://dq/results/"):
        self.spark = spark
        self.dq_results_path = dq_results_path
        self.results: list[DQCheckResult] = []

    def check_schema_compliance(
        self, df: DataFrame, expected_schema: StructType, table: str, layer: str
    ) -> DQCheckResult:
        """Verify all expected columns exist with correct types."""
        expected_fields = {f.name: f.dataType for f in expected_schema.fields}
        actual_fields = {f.name: f.dataType for f in df.schema.fields}

        missing = set(expected_fields.keys()) - set(actual_fields.keys())
        type_mismatches = {
            col: (expected_fields[col], actual_fields[col])
            for col in expected_fields
            if col in actual_fields and str(expected_fields[col]) != str(actual_fields[col])
        }

        passed = len(missing) == 0 and len(type_mismatches) == 0
        details = ""
        if missing:
            details += f"Missing columns: {missing}. "
        if type_mismatches:
            details += f"Type mismatches: {type_mismatches}."

        result = DQCheckResult(
            check_name="schema_compliance",
            table=table,
            layer=layer,
            passed=passed,
            severity="critical",
            metric_value=1.0 if passed else 0.0,
            threshold=1.0,
            details=details,
        )
        self.results.append(result)
        return result

    def check_null_rate(
        self,
        df: DataFrame,
        column: str,
        max_null_rate: float,
        table: str,
        layer: str,
        severity: str = "critical",
    ) -> DQCheckResult:
        """Check null rate for a critical column stays under threshold."""
        total = df.count()
        if total == 0:
            result = DQCheckResult(
                check_name=f"null_rate:{column}",
                table=table, layer=layer,
                passed=False, severity=severity,
                metric_value=1.0, threshold=max_null_rate,
                details="Empty table — possible upstream failure",
            )
            self.results.append(result)
            return result

        null_count = df.filter(F.col(column).isNull()).count()
        null_rate = null_count / total

        result = DQCheckResult(
            check_name=f"null_rate:{column}",
            table=table, layer=layer,
            passed=null_rate <= max_null_rate,
            severity=severity,
            metric_value=round(null_rate, 6),
            threshold=max_null_rate,
            details=f"{null_count}/{total} nulls ({null_rate*100:.3f}%)",
        )
        self.results.append(result)
        return result

    def check_referential_integrity(
        self,
        child_df: DataFrame,
        child_key: str,
        parent_df: DataFrame,
        parent_key: str,
        table: str,
        layer: str,
        max_orphan_rate: float = 0.0005,
    ) -> DQCheckResult:
        """
        Check that child_key values exist in parent_key.
        e.g., orders.customer_id must exist in customers.customer_id
        
        Uses left anti join — returns only rows in child not matching parent.
        """
        total = child_df.count()
        orphans = child_df.join(
            parent_df.select(parent_key),
            child_df[child_key] == parent_df[parent_key],
            how="left_anti"
        ).count()

        orphan_rate = orphans / total if total > 0 else 0

        result = DQCheckResult(
            check_name=f"referential_integrity:{child_key}→{parent_key}",
            table=table, layer=layer,
            passed=orphan_rate <= max_orphan_rate,
            severity="critical",
            metric_value=round(orphan_rate, 6),
            threshold=max_orphan_rate,
            details=f"{orphans} orphan records ({orphan_rate*100:.3f}%)",
        )
        self.results.append(result)
        return result

    def check_volume_anomaly(
        self,
        df: DataFrame,
        table: str,
        layer: str,
        expected_min: int,
        expected_max: int,
    ) -> DQCheckResult:
        """Detect unexpected volume spikes or drops vs. expected range."""
        actual_count = df.count()
        passed = expected_min <= actual_count <= expected_max

        result = DQCheckResult(
            check_name="volume_check",
            table=table, layer=layer,
            passed=passed,
            severity="warning",
            metric_value=float(actual_count),
            threshold=float(expected_max),
            details=f"Row count {actual_count} vs expected [{expected_min}, {expected_max}]",
        )
        self.results.append(result)
        return result

    def check_freshness(
        self,
        df: DataFrame,
        event_time_col: str,
        max_staleness_hours: int,
        table: str,
        layer: str,
    ) -> DQCheckResult:
        """Ensure most recent data is within SLA staleness threshold."""
        max_event_time = df.agg(F.max(event_time_col)).collect()[0][0]
        staleness_hours = (
            (datetime.utcnow() - max_event_time).total_seconds() / 3600
            if max_event_time else float("inf")
        )

        result = DQCheckResult(
            check_name=f"freshness:{event_time_col}",
            table=table, layer=layer,
            passed=staleness_hours <= max_staleness_hours,
            severity="critical",
            metric_value=round(staleness_hours, 2),
            threshold=float(max_staleness_hours),
            details=f"Latest event {staleness_hours:.1f}h ago (SLA: {max_staleness_hours}h)",
        )
        self.results.append(result)
        return result

    def get_summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        critical_failures = [r for r in self.results if not r.passed and r.severity == "critical"]

        return {
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "critical_failures": len(critical_failures),
            "pass_rate": passed / total if total > 0 else 0,
            "critical_failure_details": [r.details for r in critical_failures],
        }

    def should_block_promotion(self) -> bool:
        """Returns True if any CRITICAL check failed — blocking Silver→Gold promotion."""
        return any(not r.passed and r.severity == "critical" for r in self.results)

    def persist_results(self):
        """Write DQ results to Delta table for trending and alerting."""
        results_data = [
            (r.check_name, r.table, r.layer, r.passed, r.severity,
             r.metric_value, r.threshold, r.details, r.run_ts)
            for r in self.results
        ]
        df = self.spark.createDataFrame(
            results_data,
            ["check_name", "table", "layer", "passed", "severity",
             "metric_value", "threshold", "details", "run_ts"]
        )
        (
            df.write.format("delta")
            .mode("append")
            .save(self.dq_results_path)
        )
        logger.info(f"Persisted {len(self.results)} DQ results to {self.dq_results_path}")
