from __future__ import annotations

from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, functions as F


BRONZE_BASE = "s3a://bronze/source"
ICEBERG_BASE = "s3a://bronze/iceberg"
ROOT_DIR = Path(__file__).resolve().parents[1]
LOCAL_SOURCE_DIR = ROOT_DIR / "data" / "source"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("create-bronze-tables")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", ICEBERG_BASE)
        .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs-s3:8333")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def add_metadata(df, source: str, nifi_flow: str):
    return (
        df.withColumn("_source", F.lit(source))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_nifi_flow", F.lit(nifi_flow))
    )


def cluster_for_partition(df, *columns: str):
    if not columns:
        return df
    repartition_cols = [F.to_date("event_ts")] if "event_ts" in columns else [F.col(column) for column in columns]
    return df.repartition(*repartition_cols).sortWithinPartitions(*columns)


def sync_local_source_files(spark: SparkSession) -> None:
    if not LOCAL_SOURCE_DIR.exists():
        return

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    remote_base = jvm.org.apache.hadoop.fs.Path(BRONZE_BASE)
    fs.mkdirs(remote_base)

    for csv_path in sorted(LOCAL_SOURCE_DIR.glob("*.csv")):
        local_path = jvm.org.apache.hadoop.fs.Path(str(csv_path))
        remote_path = jvm.org.apache.hadoop.fs.Path(f"{BRONZE_BASE}/{csv_path.name}")
        fs.copyFromLocalFile(False, True, local_path, remote_path)


def write_table(spark: SparkSession, df, table_name: str, location: str, partition_expr: Optional[str] = None):
    temp_view = table_name.replace(".", "_")
    df.createOrReplaceTempView(temp_view)
    partition_sql = f"PARTITIONED BY ({partition_expr})" if partition_expr else ""
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {table_name}
        USING iceberg
        {partition_sql}
        LOCATION '{location}'
        AS SELECT * FROM {temp_view}
        """
    )


def main() -> None:
    spark = build_spark()
    # Source CSVs are expected to be staged under s3a://bronze/source ahead of this job.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

    mes_df = (
        spark.read.option("header", True).csv(f"{BRONZE_BASE}/mes_events.csv")
        .select(
            "event_id",
            "machine_id",
            "batch_id",
            "product_code",
            "parameter_name",
            F.col("parameter_value").cast("double").alias("parameter_value"),
            "unit",
            "operator_id",
            "shift",
            F.to_timestamp("event_ts").alias("event_ts"),
            "status",
        )
        .transform(lambda df: add_metadata(df, "MES", "csv_seed"))
        .transform(lambda df: cluster_for_partition(df, "event_ts"))
    )
    write_table(spark, mes_df, "lakehouse.bronze.mes_events", f"{ICEBERG_BASE}/mes_events", "days(event_ts)")

    iqms_df = (
        spark.read.option("header", True).csv(f"{BRONZE_BASE}/iqms_orders.csv")
        .select(
            "order_id",
            "product_code",
            "batch_id",
            F.col("quantity").cast("int").alias("quantity"),
            "uom",
            F.to_timestamp("planned_start").alias("planned_start"),
            F.to_timestamp("actual_start").alias("actual_start"),
            F.to_timestamp("actual_end").alias("actual_end"),
            "status",
            "line_id",
        )
        .transform(lambda df: add_metadata(df, "IQMS", "csv_seed"))
    )
    write_table(spark, iqms_df, "lakehouse.bronze.iqms_orders", f"{ICEBERG_BASE}/iqms_orders")

    trackwise_df = (
        spark.read.option("header", True).csv(f"{BRONZE_BASE}/trackwise_deviations.csv")
        .select(
            "deviation_id",
            "batch_id",
            "product_code",
            "deviation_type",
            "severity",
            "description",
            "reported_by",
            F.to_timestamp("reported_ts").alias("reported_ts"),
            "status",
            F.to_timestamp("resolution_ts").alias("resolution_ts"),
        )
        .transform(lambda df: add_metadata(df, "TrackWise", "csv_seed"))
    )
    write_table(spark, trackwise_df, "lakehouse.bronze.trackwise_deviations", f"{ICEBERG_BASE}/trackwise_deviations")

    sap_df = (
        spark.read.option("header", True).csv(f"{BRONZE_BASE}/sap_ecc_orders.csv")
        .select(
            "po_number",
            "material_code",
            "plant",
            "storage_location",
            F.col("planned_qty").cast("int").alias("planned_qty"),
            F.col("actual_qty").cast("int").alias("actual_qty"),
            "uom",
            F.to_date("posting_date").alias("posting_date"),
            "cost_center",
            F.col("total_cost").cast("double").alias("total_cost"),
            "currency",
        )
        .transform(lambda df: add_metadata(df, "SAP ECC", "csv_seed"))
    )
    write_table(spark, sap_df, "lakehouse.bronze.sap_ecc_orders", f"{ICEBERG_BASE}/sap_ecc_orders")

    sop_df = (
        spark.read.option("header", True).csv(f"{BRONZE_BASE}/sop_documents.csv")
        .select(
            "doc_id",
            "doc_type",
            "title",
            "version",
            F.to_date("effective_date").alias("effective_date"),
            "author",
            "department",
            "file_path",
            F.col("page_count").cast("int").alias("page_count"),
        )
        .transform(lambda df: add_metadata(df, "Document Index", "csv_seed"))
    )
    write_table(spark, sop_df, "lakehouse.bronze.sop_documents", f"{ICEBERG_BASE}/sop_documents")

    # Create missing tables for dbt compatibility
    # In a real scenario, these would be populated via Kafka or more CSVs.
    spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.bronze.tms_training_completions (record_id STRING, employee_id STRING, employee_name STRING, department STRING, training_name STRING, training_category STRING, scheduled_date STRING, completion_date STRING, score STRING, status STRING, trainer_id STRING, training_mode STRING, validity_months STRING, _source STRING, _ingested_at TIMESTAMP) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.bronze.trackwise_capas (_kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING, _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.bronze.mes_production_orders (_kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING, _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.bronze.iqms_quality_tests (_kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING, _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.bronze.iqms_deviations (_kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING, _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT) USING iceberg")

    verification = spark.sql(
        """
        SELECT COUNT(*) AS row_count, MIN(event_ts) AS min_event_ts, MAX(event_ts) AS max_event_ts
        FROM lakehouse.bronze.mes_events
        """
    )
    verification.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
