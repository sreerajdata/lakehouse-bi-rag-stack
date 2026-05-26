from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import psycopg2
from pyspark.sql import SparkSession, Window, functions as F


BRONZE_SOURCE_BASE = "s3a://bronze/source"
NIFI_INGEST_BASE = "s3a://bronze/nifi-ingest-v3"
BRONZE_WAREHOUSE = "s3a://lakehouse-bronze/warehouse"
ROOT_DIR = Path(__file__).resolve().parents[1]
LOCAL_SOURCE_DIR = ROOT_DIR / "data" / "source"
MANIFEST_PATH = ROOT_DIR / "data" / ".bronze_source_manifest.json"
PRIMARY_SOURCE_FILES = [
    "mes_events.csv",
    "iqms_orders.csv",
    "trackwise_deviations.csv",
    "sap_ecc_orders.csv",
    "sop_documents.csv",
]
METASTORE_DSN = {
    "host": "postgres",
    "dbname": "hive_metastore",
    "user": "admin",
    "password": "admin123",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("create-bronze-tables")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", BRONZE_WAREHOUSE)
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


def build_source_manifest() -> dict[str, dict[str, int]]:
    manifest: dict[str, dict[str, int]] = {}
    for filename in PRIMARY_SOURCE_FILES:
        path = LOCAL_SOURCE_DIR / filename
        if not path.exists():
            continue
        stat = path.stat()
        manifest[filename] = {
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    return manifest


def load_previous_manifest() -> dict[str, dict[str, int]]:
    if not MANIFEST_PATH.exists():
        return {}
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def save_manifest(manifest: dict[str, dict[str, int]]) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def changed_source_files(
    current_manifest: dict[str, dict[str, int]],
    previous_manifest: dict[str, dict[str, int]],
) -> list[str]:
    return [
        filename
        for filename in PRIMARY_SOURCE_FILES
        if current_manifest.get(filename) != previous_manifest.get(filename)
    ]


def sync_local_source_files(spark: SparkSession, filenames: list[str]) -> None:
    if not LOCAL_SOURCE_DIR.exists():
        return

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    remote_fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(BRONZE_SOURCE_BASE), hadoop_conf)
    local_fs = jvm.org.apache.hadoop.fs.FileSystem.getLocal(hadoop_conf)
    remote_base = jvm.org.apache.hadoop.fs.Path(BRONZE_SOURCE_BASE)
    remote_fs.mkdirs(remote_base)

    for filename in filenames:
        csv_path = LOCAL_SOURCE_DIR / filename
        if not csv_path.exists():
            continue
        local_path = jvm.org.apache.hadoop.fs.Path(str(csv_path))
        remote_path = jvm.org.apache.hadoop.fs.Path(f"{BRONZE_SOURCE_BASE}/{csv_path.name}")
        jvm.org.apache.hadoop.fs.FileUtil.copy(local_fs, local_path, remote_fs, remote_path, False, True, hadoop_conf)


def nifi_patterns(filename: str) -> list[str]:
    return [
        f"{NIFI_INGEST_BASE}/{filename}/*.json",
    ]


def matching_nifi_patterns(spark: SparkSession, filename: str) -> list[str]:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(NIFI_INGEST_BASE), hadoop_conf)
    matched_patterns = []
    for location_pattern in nifi_patterns(filename):
        matches = fs.globStatus(jvm.org.apache.hadoop.fs.Path(location_pattern))
        if matches:
            matched_patterns.append(location_pattern)
    return matched_patterns


def read_preferred_source(spark: SparkSession, filename: str, select_exprs: list, source_name: str):
    nifi_locations = matching_nifi_patterns(spark, filename)
    if nifi_locations:
        nifi_df = (
            spark.read.option("multiline", True).json(nifi_locations)
            .select(*select_exprs)
            .transform(lambda df: add_metadata(df, source_name, "nifi_s3_ingest"))
        )
        if nifi_df.take(1):
            print(f"Using NiFi-ingested objects for {filename}")
            return nifi_df

    print(f"Using staged CSV fallback for {filename}")
    return (
        spark.read.option("header", True).csv(f"{BRONZE_SOURCE_BASE}/{filename}")
        .select(*select_exprs)
        .transform(lambda df: add_metadata(df, source_name, "csv_seed"))
    )


def deduplicate_rows(df, key_columns: list[str], order_column: Optional[str] = None):
    if not key_columns:
        return df

    if order_column:
        window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc_nulls_last())
        return df.withColumn("_row_rank", F.row_number().over(window)).where(F.col("_row_rank") == 1).drop("_row_rank")

    return df.dropDuplicates(key_columns)


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


def purge_metastore_table(schema_name: str, table_name: str) -> int:
    with psycopg2.connect(**METASTORE_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t."TBL_ID", t."SD_ID", s."SERDE_ID", s."CD_ID"
                FROM "TBLS" t
                JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
                LEFT JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
                WHERE d."NAME" = %s AND t."TBL_NAME" = %s
                """,
                (schema_name, table_name),
            )
            rows = cur.fetchall()
            if not rows:
                return 0

            tbl_ids = [row[0] for row in rows if row[0] is not None]
            sd_ids = [row[1] for row in rows if row[1] is not None]
            serde_ids = [row[2] for row in rows if row[2] is not None]
            cd_ids = [row[3] for row in rows if row[3] is not None]

            cur.execute('DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" = ANY(%s)', (tbl_ids,))
            cur.execute('DELETE FROM "PARTITION_KEYS" WHERE "TBL_ID" = ANY(%s)', (tbl_ids,))
            cur.execute('DELETE FROM "PARTITIONS" WHERE "TBL_ID" = ANY(%s)', (tbl_ids,))
            cur.execute('DELETE FROM "TBLS" WHERE "TBL_ID" = ANY(%s)', (tbl_ids,))

            if sd_ids:
                cur.execute('DELETE FROM "SD_PARAMS" WHERE "SD_ID" = ANY(%s)', (sd_ids,))
                cur.execute('DELETE FROM "SORT_COLS" WHERE "SD_ID" = ANY(%s)', (sd_ids,))
                cur.execute('DELETE FROM "BUCKETING_COLS" WHERE "SD_ID" = ANY(%s)', (sd_ids,))
                cur.execute('DELETE FROM "SKEWED_COL_NAMES" WHERE "SD_ID" = ANY(%s)', (sd_ids,))
                cur.execute('DELETE FROM "SKEWED_VALUES" WHERE "SD_ID_OID" = ANY(%s)', (sd_ids,))
                cur.execute(
                    """
                    DELETE FROM "SDS" s
                    WHERE s."SD_ID" = ANY(%s)
                      AND NOT EXISTS (SELECT 1 FROM "TBLS" t WHERE t."SD_ID" = s."SD_ID")
                      AND NOT EXISTS (SELECT 1 FROM "PARTITIONS" p WHERE p."SD_ID" = s."SD_ID")
                    """,
                    (sd_ids,),
                )

            if serde_ids:
                cur.execute(
                    """
                    DELETE FROM "SERDE_PARAMS" sp
                    WHERE sp."SERDE_ID" = ANY(%s)
                      AND NOT EXISTS (SELECT 1 FROM "SDS" s WHERE s."SERDE_ID" = sp."SERDE_ID")
                    """,
                    (serde_ids,),
                )
                cur.execute(
                    """
                    DELETE FROM "SERDES" se
                    WHERE se."SERDE_ID" = ANY(%s)
                      AND NOT EXISTS (SELECT 1 FROM "SDS" s WHERE s."SERDE_ID" = se."SERDE_ID")
                    """,
                    (serde_ids,),
                )

            if cd_ids:
                cur.execute(
                    """
                    DELETE FROM "COLUMNS_V2" c
                    WHERE c."CD_ID" = ANY(%s)
                      AND NOT EXISTS (SELECT 1 FROM "SDS" s WHERE s."CD_ID" = c."CD_ID")
                    """,
                    (cd_ids,),
                )
                cur.execute(
                    """
                    DELETE FROM "CDS" cd
                    WHERE cd."CD_ID" = ANY(%s)
                      AND NOT EXISTS (SELECT 1 FROM "SDS" s WHERE s."CD_ID" = cd."CD_ID")
                    """,
                    (cd_ids,),
                )

            conn.commit()
            return len(tbl_ids)


def create_placeholder_if_not_exists(spark: SparkSession, table_name: str, ddl_body: str, location: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {ddl_body}
        )
        USING iceberg
        LOCATION '{location}'
        """
    )


def main() -> None:
    current_manifest = build_source_manifest()
    previous_manifest = load_previous_manifest()
    files_to_refresh = changed_source_files(current_manifest, previous_manifest)
    if current_manifest and not files_to_refresh:
        print("Primary Bronze source files unchanged; skipping Spark Bronze rebuild.")
        return

    spark = build_spark()
    sync_local_source_files(spark, files_to_refresh)
    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze
        LOCATION '{BRONZE_WAREHOUSE}/bronze.db'
        """
    )

    if "mes_events.csv" in files_to_refresh:
        mes_df = (
            read_preferred_source(
                spark,
                "mes_events.csv",
                [
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
                ],
                "MES",
            )
            .transform(lambda df: deduplicate_rows(df, ["event_id"], "event_ts"))
            .transform(lambda df: cluster_for_partition(df, "event_ts"))
        )
        write_table(
            spark,
            mes_df,
            "lakehouse.bronze.mes_events",
            f"{BRONZE_WAREHOUSE}/bronze.db/mes_events",
            "days(event_ts)",
        )

    if "iqms_orders.csv" in files_to_refresh:
        iqms_df = (
            read_preferred_source(
                spark,
                "iqms_orders.csv",
                [
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
                ],
                "IQMS",
            )
            .transform(lambda df: deduplicate_rows(df, ["order_id"], "actual_start"))
        )
        write_table(spark, iqms_df, "lakehouse.bronze.iqms_orders", f"{BRONZE_WAREHOUSE}/bronze.db/iqms_orders")

    if "trackwise_deviations.csv" in files_to_refresh:
        trackwise_df = (
            read_preferred_source(
                spark,
                "trackwise_deviations.csv",
                [
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
                ],
                "TrackWise",
            )
            .transform(lambda df: deduplicate_rows(df, ["deviation_id"], "reported_ts"))
        )
        write_table(
            spark,
            trackwise_df,
            "lakehouse.bronze.trackwise_deviations",
            f"{BRONZE_WAREHOUSE}/bronze.db/trackwise_deviations",
        )

    if "sap_ecc_orders.csv" in files_to_refresh:
        sap_df = (
            read_preferred_source(
                spark,
                "sap_ecc_orders.csv",
                [
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
                ],
                "SAP ECC",
            )
            .transform(lambda df: deduplicate_rows(df, ["po_number"], "posting_date"))
        )
        write_table(
            spark,
            sap_df,
            "lakehouse.bronze.sap_ecc_orders",
            f"{BRONZE_WAREHOUSE}/bronze.db/sap_ecc_orders",
        )

    if "sop_documents.csv" in files_to_refresh:
        sop_df = (
            read_preferred_source(
                spark,
                "sop_documents.csv",
                [
                    "doc_id",
                    "doc_type",
                    "title",
                    "version",
                    F.to_date("effective_date").alias("effective_date"),
                    "author",
                    "department",
                    "file_path",
                    F.col("page_count").cast("int").alias("page_count"),
                ],
                "Document Index",
            )
            .transform(lambda df: deduplicate_rows(df, ["doc_id"], "effective_date"))
        )
        write_table(
            spark,
            sop_df,
            "lakehouse.bronze.sop_documents",
            f"{BRONZE_WAREHOUSE}/bronze.db/sop_documents",
        )

    create_placeholder_if_not_exists(
        spark,
        "lakehouse.bronze.tms_training_completions",
        """
        record_id STRING, employee_id STRING, employee_name STRING, department STRING,
        training_name STRING, training_category STRING, scheduled_date STRING,
        completion_date STRING, score STRING, status STRING, trainer_id STRING,
        training_mode STRING, validity_months STRING, _source STRING, _ingested_at TIMESTAMP
        """,
        f"{BRONZE_WAREHOUSE}/bronze.db/tms_training_completions",
    )
    create_placeholder_if_not_exists(
        spark,
        "lakehouse.bronze.trackwise_capas",
        """
        _kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING,
        _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT
        """,
        f"{BRONZE_WAREHOUSE}/bronze.db/trackwise_capas",
    )
    create_placeholder_if_not_exists(
        spark,
        "lakehouse.bronze.mes_production_orders",
        """
        _kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING,
        _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT
        """,
        f"{BRONZE_WAREHOUSE}/bronze.db/mes_production_orders",
    )
    create_placeholder_if_not_exists(
        spark,
        "lakehouse.bronze.iqms_quality_tests",
        """
        _kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING,
        _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT
        """,
        f"{BRONZE_WAREHOUSE}/bronze.db/iqms_quality_tests",
    )
    create_placeholder_if_not_exists(
        spark,
        "lakehouse.bronze.iqms_deviations",
        """
        _kafka_key STRING, _raw_payload STRING, _ingested_at TIMESTAMP, _row_hash STRING,
        _kafka_offset LONG, _source_system STRING, _ingest_year INT, _ingest_month INT, _ingest_day INT
        """,
        f"{BRONZE_WAREHOUSE}/bronze.db/iqms_deviations",
    )

    verification = spark.sql(
        """
        SELECT COUNT(*) AS row_count, MIN(event_ts) AS min_event_ts, MAX(event_ts) AS max_event_ts
        FROM lakehouse.bronze.mes_events
        """
    )
    verification.show(truncate=False)
    spark.stop()
    save_manifest(current_manifest)


if __name__ == "__main__":
    main()
