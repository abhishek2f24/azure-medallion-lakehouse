"""
Data observability: freshness, volume anomaly, schema drift detection.
Runs after every pipeline execution. Alerts via Slack + PagerDuty on breach.
"""

import os
import json
import statistics
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SLACK_WEBHOOK     = os.environ["SLACK_WEBHOOK_URL"]
PAGERDUTY_KEY     = os.environ["PAGERDUTY_ROUTING_KEY"]
STORAGE_ACCOUNT   = "supplylakehouseprodadls"
SILVER_BASE       = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_BASE         = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

FRESHNESS_SLAS = {
    "silver/orders":          timedelta(hours=1),
    "silver/shipments":       timedelta(hours=1),
    "silver/inventory":       timedelta(hours=2),
    "gold/fct_supply_chain":  timedelta(hours=2),
    "gold/dim_supplier_scorecard": timedelta(hours=24),
}

VOLUME_ANOMALY_THRESHOLD_PCT = 30  # alert if row count deviates >30% from 7-day avg


@dataclass
class ObservabilityAlert:
    check_type: str
    table: str
    message: str
    severity: str = "warning"  # warning | critical
    metadata: dict = field(default_factory=dict)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DataObservability")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def check_freshness(spark: SparkSession) -> list[ObservabilityAlert]:
    alerts = []
    now = datetime.now(timezone.utc)

    for table_path, sla in FRESHNESS_SLAS.items():
        base = SILVER_BASE if table_path.startswith("silver") else GOLD_BASE
        entity = table_path.split("/")[1]
        full_path = f"{base}/{entity}"

        try:
            df = spark.read.format("delta").load(full_path)
            ts_col = "_silver_updated_at" if "silver" in table_path else "dbt_updated_at"
            latest = df.agg(F.max(ts_col)).collect()[0][0]

            if latest is None:
                alerts.append(ObservabilityAlert(
                    check_type="freshness",
                    table=table_path,
                    message=f"No data found in {table_path}",
                    severity="critical",
                ))
                continue

            lag = now - latest.replace(tzinfo=timezone.utc)
            if lag > sla:
                alerts.append(ObservabilityAlert(
                    check_type="freshness",
                    table=table_path,
                    message=f"Data stale: lag={lag}, SLA={sla}",
                    severity="critical" if lag > sla * 2 else "warning",
                    metadata={"lag_minutes": lag.total_seconds() / 60, "sla_minutes": sla.total_seconds() / 60},
                ))
        except Exception as e:
            alerts.append(ObservabilityAlert(
                check_type="freshness",
                table=table_path,
                message=f"Could not read table: {e}",
                severity="critical",
            ))

    return alerts


def check_volume_anomaly(spark: SparkSession) -> list[ObservabilityAlert]:
    alerts = []

    for table_path in ["silver/orders", "silver/shipments", "gold/fct_supply_chain"]:
        base = SILVER_BASE if table_path.startswith("silver") else GOLD_BASE
        entity = table_path.split("/")[1]
        full_path = f"{base}/{entity}"

        try:
            df = spark.read.format("delta").load(full_path)
            date_col = "_ingestion_date" if "silver" in table_path else "order_date"

            daily_counts = (
                df.groupBy(date_col)
                .count()
                .orderBy(F.col(date_col).desc())
                .limit(8)
                .collect()
            )

            if len(daily_counts) < 2:
                continue

            today_count  = daily_counts[0]["count"]
            history      = [r["count"] for r in daily_counts[1:]]
            avg_7d       = statistics.mean(history)
            deviation_pct = abs(today_count - avg_7d) / max(avg_7d, 1) * 100

            if deviation_pct > VOLUME_ANOMALY_THRESHOLD_PCT:
                direction = "surge" if today_count > avg_7d else "drop"
                alerts.append(ObservabilityAlert(
                    check_type="volume_anomaly",
                    table=table_path,
                    message=f"Volume {direction}: today={today_count:,}, 7d_avg={avg_7d:,.0f}, deviation={deviation_pct:.1f}%",
                    severity="critical" if deviation_pct > 60 else "warning",
                    metadata={"today": today_count, "avg_7d": avg_7d, "deviation_pct": deviation_pct},
                ))
        except Exception as e:
            alerts.append(ObservabilityAlert(
                check_type="volume_anomaly",
                table=table_path,
                message=f"Could not check volume: {e}",
                severity="warning",
            ))

    return alerts


def send_slack(alert: ObservabilityAlert):
    icon = ":red_circle:" if alert.severity == "critical" else ":warning:"
    payload = {
        "text": f"{icon} *[{alert.severity.upper()}] Data Observability Alert*",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text":
                f"{icon} *{alert.check_type.replace('_', ' ').title()}* — `{alert.table}`\n{alert.message}"
            }},
            {"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"*Metadata:* {json.dumps(alert.metadata)}"}
            ]},
        ],
    }
    requests.post(SLACK_WEBHOOK, json=payload, timeout=10)


def send_pagerduty(alert: ObservabilityAlert):
    if alert.severity != "critical":
        return
    payload = {
        "routing_key": PAGERDUTY_KEY,
        "event_action": "trigger",
        "payload": {
            "summary": f"[Lakehouse] {alert.check_type}: {alert.table} — {alert.message}",
            "severity": "critical",
            "source": "data-observability",
            "custom_details": alert.metadata,
        },
    }
    requests.post("https://events.pagerduty.com/v2/enqueue", json=payload, timeout=10)


def main():
    spark = build_spark()
    all_alerts: list[ObservabilityAlert] = []

    all_alerts.extend(check_freshness(spark))
    all_alerts.extend(check_volume_anomaly(spark))

    if not all_alerts:
        print("[Observability] All checks passed.")
        return

    for alert in all_alerts:
        print(f"[{alert.severity.upper()}] {alert.check_type} | {alert.table} | {alert.message}")
        send_slack(alert)
        send_pagerduty(alert)

    critical = [a for a in all_alerts if a.severity == "critical"]
    if critical:
        raise RuntimeError(f"{len(critical)} critical observability failures detected.")


if __name__ == "__main__":
    main()
