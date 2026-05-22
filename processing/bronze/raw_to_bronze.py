"""
Bronze layer: Raw → Bronze

Reads raw files from ADLS (Parquet/CSV/JSON), enforces schemas,
partitions by ingestion_date, writes Delta Lake with full audit trail.
Bad records → quarantine table (never dropped).
"""

import sys
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType, LongType
)
from delta.tables import DeltaTable


STORAGE_ACCOUNT = "supplylakehouseprodadls"
ADLS_BASE = f"abfss://{{container}}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

BRONZE_BASE  = ADLS_BASE.format(container="bronze")
RAW_BASE     = ADLS_BASE.format(container="raw")
QUARANTINE   = ADLS_BASE.format(container="quarantine")


SCHEMAS = {
    "orders": StructType([
        StructField("order_id",        StringType(),    False),
        StructField("customer_id",     StringType(),    True),
        StructField("supplier_id",     StringType(),    True),
        StructField("product_id",      StringType(),    True),
        StructField("order_date",      StringType(),    True),
        StructField("required_date",   StringType(),    True),
        StructField("quantity",        IntegerType(),   True),
        StructField("unit_price",      DoubleType(),    True),
        StructField("currency",        StringType(),    True),
        StructField("status",          StringType(),    True),
        StructField("region",          StringType(),    True),
        StructField("source_system",   StringType(),    True),
    ]),
    "shipments": StructType([
        StructField("shipment_id",     StringType(),    False),
        StructField("order_id",        StringType(),    True),
        StructField("carrier",         StringType(),    True),
        StructField("tracking_number", StringType(),    True),
        StructField("origin",          StringType(),    True),
        StructField("destination",     StringType(),    True),
        StructField("shipped_at",      StringType(),    True),
        StructField("estimated_at",    StringType(),    True),
        StructField("delivered_at",    StringType(),    True),
        StructField("status",          StringType(),    True),
        StructField("weight_kg",       DoubleType(),    True),
        StructField("freight_cost",    DoubleType(),    True),
    ]),
    "inventory": StructType([
        StructField("snapshot_id",     StringType(),    False),
        StructField("warehouse_id",    StringType(),    True),
        StructField("product_id",      StringType(),    True),
        StructField("quantity_on_hand",IntegerType(),   True),
        StructField("quantity_reserved",IntegerType(),  True),
        StructField("reorder_point",   IntegerType(),   True),
        StructField("snapshot_date",   StringType(),    True),
        StructField("last_movement",   StringType(),    True),
    ]),
    "suppliers": StructType([
        StructField("supplier_id",     StringType(),    False),
        StructField("supplier_name",   StringType(),    True),
        StructField("country",         StringType(),    True),
        StructField("region",          StringType(),    True),
        StructField("category",        StringType(),    True),
        StructField("tier",            StringType(),    True),
        StructField("contract_start",  StringType(),    True),
        StructField("contract_end",    StringType(),    True),
        StructField("active",          StringType(),    True),
    ]),
}

PK_COLUMNS = {
    "orders":    "order_id",
    "shipments": "shipment_id",
    "inventory": "snapshot_id",
    "suppliers": "supplier_id",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BronzeLayer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, entity: str, ingestion_date: str) -> DataFrame:
    path = f"{RAW_BASE}/{entity}/{ingestion_date}/"
    schema = SCHEMAS[entity]

    # Try Parquet first, fall back to CSV
    try:
        return spark.read.schema(schema).parquet(path)
    except Exception:
        return (
            spark.read
            .schema(schema)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(path)
        )


def split_valid_quarantine(df: DataFrame, entity: str) -> tuple[DataFrame, DataFrame]:
    pk = PK_COLUMNS[entity]
    valid = df.filter(F.col(pk).isNotNull())
    bad = (
        df.filter(F.col(pk).isNull())
        .select(
            F.to_json(F.struct("*")).alias("raw_record"),
            F.lit(entity).alias("source_entity"),
            F.lit("null_primary_key").alias("quarantine_reason"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )
    return valid, bad


def write_bronze(df: DataFrame, entity: str, ingestion_date: str):
    output_path = f"{BRONZE_BASE}/{entity}"

    enriched = (
        df
        .withColumn("_ingestion_date",      F.lit(ingestion_date).cast(DateType()))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file",         F.input_file_name())
    )

    if DeltaTable.isDeltaTable(SparkSession.getActiveSession(), output_path):
        pk = PK_COLUMNS[entity]
        dt = DeltaTable.forPath(SparkSession.getActiveSession(), output_path)
        (
            dt.alias("t")
            .merge(enriched.alias("s"), f"t.{pk} = s.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            enriched.write
            .format("delta")
            .partitionBy("_ingestion_date")
            .save(output_path)
        )

    print(f"[Bronze] {entity}: {df.count():,} rows written → {output_path}")


def write_quarantine(df: DataFrame, entity: str, ingestion_date: str):
    if df.head(1):
        path = f"{QUARANTINE}/{entity}/{ingestion_date}"
        df.write.format("delta").mode("append").save(path)
        print(f"[Quarantine] {entity}: bad records written → {path}")


def process_entity(spark: SparkSession, entity: str, ingestion_date: str):
    print(f"[Bronze] Processing: {entity} for {ingestion_date}")
    raw = read_raw(spark, entity, ingestion_date)
    valid, bad = split_valid_quarantine(raw, entity)
    write_bronze(valid, entity, ingestion_date)
    write_quarantine(bad, entity, ingestion_date)


def main():
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
    entities = sys.argv[2:] if len(sys.argv) > 2 else list(SCHEMAS.keys())

    spark = build_spark()

    for entity in entities:
        process_entity(spark, entity, ingestion_date)

    print(f"[Bronze] Complete for {ingestion_date}.")


if __name__ == "__main__":
    main()
