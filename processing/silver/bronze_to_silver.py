"""
Silver layer: Bronze → Silver

Per entity:
  - Deduplication (latest record wins per PK)
  - Type casting and standardisation
  - SCD Type 2 for slowly-changing dimensions (suppliers)
  - Referential integrity checks
  - Business rule validation
  - MERGE into Silver Delta tables
"""

import sys
from datetime import date
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType, DoubleType, IntegerType
from delta.tables import DeltaTable


STORAGE_ACCOUNT = "supplylakehouseprodadls"
ADLS_BASE  = f"abfss://{{container}}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_BASE = ADLS_BASE.format(container="bronze")
SILVER_BASE = ADLS_BASE.format(container="silver")
QUARANTINE  = ADLS_BASE.format(container="quarantine")

VALID_ORDER_STATUSES   = {"pending", "confirmed", "in_production", "shipped", "delivered", "cancelled"}
VALID_SHIPMENT_STATUSES = {"in_transit", "out_for_delivery", "delivered", "failed", "returned"}
VALID_CURRENCIES       = {"USD", "GBP", "EUR", "AUD", "CAD", "JPY", "INR"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SilverLayer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, entity: str, ingestion_date: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(f"{BRONZE_BASE}/{entity}")
        .filter(F.col("_ingestion_date") == ingestion_date)
    )


def deduplicate(df: DataFrame, pk: str, order_cols: list[str]) -> DataFrame:
    w = Window.partitionBy(pk).orderBy(*[F.col(c).desc() for c in order_cols])
    return (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ── Orders ────────────────────────────────────────────────────────────────────
def transform_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_statuses = F.array(*[F.lit(s) for s in VALID_ORDER_STATUSES])
    valid_currencies = F.array(*[F.lit(c) for c in VALID_CURRENCIES])

    cleaned = (
        df
        .withColumn("order_date",    F.to_date("order_date"))
        .withColumn("required_date", F.to_date("required_date"))
        .withColumn("quantity",      F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",    F.col("unit_price").cast(DoubleType()))
        .withColumn("total_value",   F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("currency",      F.upper("currency"))
        .withColumn("status",        F.lower("status"))
        .withColumn("is_urgent",     F.datediff("required_date", "order_date") <= 7)
        .withColumn("order_tier",
            F.when(F.col("total_value") >= 100000, "enterprise")
             .when(F.col("total_value") >= 10000,  "commercial")
             .when(F.col("total_value") >= 1000,   "standard")
             .otherwise("micro")
        )
    )

    valid = (
        cleaned
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .filter(F.array_contains(valid_statuses, F.col("status")))
        .filter(F.array_contains(valid_currencies, F.col("currency")))
        .filter(F.col("order_date").isNotNull())
    )

    invalid = (
        cleaned
        .subtract(valid)
        .select(
            F.to_json(F.struct("*")).alias("raw_record"),
            F.lit("orders").alias("source_entity"),
            F.lit("silver_validation_failed").alias("quarantine_reason"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )

    return deduplicate(valid, "order_id", ["_ingestion_timestamp"]), invalid


# ── Shipments ─────────────────────────────────────────────────────────────────
def transform_shipments(df: DataFrame) -> DataFrame:
    return (
        deduplicate(df, "shipment_id", ["_ingestion_timestamp"])
        .withColumn("shipped_at",    F.to_timestamp("shipped_at"))
        .withColumn("estimated_at",  F.to_timestamp("estimated_at"))
        .withColumn("delivered_at",  F.to_timestamp("delivered_at"))
        .withColumn("weight_kg",     F.col("weight_kg").cast(DoubleType()))
        .withColumn("freight_cost",  F.col("freight_cost").cast(DoubleType()))
        .withColumn("transit_days",
            F.when(
                F.col("delivered_at").isNotNull(),
                F.datediff(F.col("delivered_at").cast(DateType()), F.col("shipped_at").cast(DateType()))
            )
        )
        .withColumn("is_on_time",
            F.when(
                F.col("delivered_at").isNotNull() & F.col("estimated_at").isNotNull(),
                F.col("delivered_at") <= F.col("estimated_at")
            ).otherwise(F.lit(None))
        )
        .withColumn("is_delayed",
            F.when(F.col("is_on_time").isNotNull(), ~F.col("is_on_time")).otherwise(F.lit(None))
        )
        .filter(F.col("order_id").isNotNull())
    )


# ── Inventory ─────────────────────────────────────────────────────────────────
def transform_inventory(df: DataFrame) -> DataFrame:
    return (
        deduplicate(df, "snapshot_id", ["_ingestion_timestamp"])
        .withColumn("snapshot_date",       F.to_date("snapshot_date"))
        .withColumn("last_movement",       F.to_date("last_movement"))
        .withColumn("quantity_available",  F.col("quantity_on_hand") - F.col("quantity_reserved"))
        .withColumn("is_below_reorder",    F.col("quantity_available") < F.col("reorder_point"))
        .withColumn("days_since_movement", F.datediff(F.current_date(), F.col("last_movement")))
        .withColumn("stock_status",
            F.when(F.col("quantity_available") <= 0,                   "out_of_stock")
             .when(F.col("is_below_reorder"),                          "low_stock")
             .when(F.col("quantity_available") < F.col("reorder_point") * 2, "adequate")
             .otherwise("well_stocked")
        )
    )


# ── Suppliers (SCD Type 2) ────────────────────────────────────────────────────
def apply_scd2_suppliers(spark: SparkSession, incoming: DataFrame) -> None:
    silver_path = f"{SILVER_BASE}/suppliers"
    pk = "supplier_id"

    incoming_clean = (
        incoming
        .withColumn("contract_start", F.to_date("contract_start"))
        .withColumn("contract_end",   F.to_date("contract_end"))
        .withColumn("active",         F.col("active").cast("boolean"))
        .withColumn("is_current",     F.lit(True))
        .withColumn("valid_from",     F.current_date())
        .withColumn("valid_to",       F.lit(None).cast(DateType()))
        .withColumn("_silver_updated_at", F.current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, silver_path):
        incoming_clean.write.format("delta").partitionBy("active").save(silver_path)
        return

    silver_dt = DeltaTable.forPath(spark, silver_path)

    # Expire changed records
    (
        silver_dt.alias("t")
        .merge(
            incoming_clean.alias("s"),
            f"t.{pk} = s.{pk} AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition="t.supplier_name <> s.supplier_name OR t.tier <> s.tier OR t.country <> s.country",
            set={"is_current": "false", "valid_to": "current_date()"}
        )
        .execute()
    )

    # Insert new/changed versions
    existing_current = silver_dt.toDF().filter("is_current = true").select(pk)
    new_rows = incoming_clean.join(existing_current, pk, "left_anti")
    changed  = incoming_clean.join(
        silver_dt.toDF().filter("valid_to = current_date()").select(pk),
        pk, "inner"
    )
    new_rows.union(changed).write.format("delta").mode("append").save(silver_path)


def merge_to_silver(spark: SparkSession, df: DataFrame, entity: str, pk: str):
    path = f"{SILVER_BASE}/{entity}"
    enriched = df.withColumn("_silver_updated_at", F.current_timestamp())

    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("t")
            .merge(enriched.alias("s"), f"t.{pk} = s.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        enriched.write.format("delta").save(path)

    print(f"[Silver] {entity}: merged {df.count():,} rows → {path}")


def main():
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
    spark = build_spark()

    # Orders
    raw_orders = read_bronze(spark, "orders", ingestion_date)
    clean_orders, bad_orders = transform_orders(raw_orders)
    merge_to_silver(spark, clean_orders, "orders", "order_id")
    if bad_orders.head(1):
        bad_orders.write.format("delta").mode("append").save(f"{QUARANTINE}/silver_orders/{ingestion_date}")

    # Shipments
    raw_shipments = read_bronze(spark, "shipments", ingestion_date)
    clean_shipments = transform_shipments(raw_shipments)
    merge_to_silver(spark, clean_shipments, "shipments", "shipment_id")

    # Inventory
    raw_inventory = read_bronze(spark, "inventory", ingestion_date)
    clean_inventory = transform_inventory(raw_inventory)
    merge_to_silver(spark, clean_inventory, "inventory", "snapshot_id")

    # Suppliers (SCD2)
    raw_suppliers = read_bronze(spark, "suppliers", ingestion_date)
    apply_scd2_suppliers(spark, raw_suppliers)
    print(f"[Silver] suppliers: SCD2 applied.")

    print(f"[Silver] Complete for {ingestion_date}.")


if __name__ == "__main__":
    main()
