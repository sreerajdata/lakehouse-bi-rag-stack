from __future__ import annotations

import argparse
import json
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from pyspark.sql import DataFrame, SparkSession, functions as F


KAFKA_SERVERS = "kafka:9092"
WAREHOUSE = "s3a://lakehouse-bronze/warehouse"
OFFSET_STATE_PATH = Path("/opt/airflow/data/.kafka_bronze_offsets.json")

RAW_TOPIC_TABLES = {
    "mes.production_orders": "lakehouse.bronze.mes_production_orders",
    "cdc.mes.public.mes_production_orders": "lakehouse.bronze.mes_production_orders",
    "iqms.quality_tests": "lakehouse.bronze.iqms_quality_tests",
    "cdc.iqms.public.iqms_quality_tests": "lakehouse.bronze.iqms_quality_tests",
    "iqms.deviations": "lakehouse.bronze.iqms_deviations",
    "trackwise.capas": "lakehouse.bronze.trackwise_capas",
}

STRUCTURED_TOPIC_TABLES = {
    "sap.inventory_movements": "lakehouse.bronze.sap_ecc_orders",
    "tms.training_completions": "lakehouse.bronze.tms_training_completions",
    "cdc.tms.public.tms_training_completions": "lakehouse.bronze.tms_training_completions",
}

ALL_TOPICS = {**RAW_TOPIC_TABLES, **STRUCTURED_TOPIC_TABLES}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze-kafka-to-iceberg")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs-s3:8333")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def load_state() -> dict[str, dict[str, int]]:
    if not OFFSET_STATE_PATH.exists():
        return {}
    return json.loads(OFFSET_STATE_PATH.read_text(encoding="utf-8"))


def save_state(state: dict[str, dict[str, int]]) -> None:
    OFFSET_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def topic_high_watermarks(topics: list[str]) -> dict[str, dict[int, int]]:
    admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
    metadata = admin.list_topics(timeout=20)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": "bronze-kafka-offset-discovery",
            "enable.auto.commit": False,
        }
    )
    try:
        watermarks: dict[str, dict[int, int]] = {}
        for topic in topics:
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                print(f"Skipping missing Kafka topic: {topic}")
                continue
            watermarks[topic] = {}
            for partition_id in sorted(topic_meta.partitions):
                _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10)
                watermarks[topic][partition_id] = high
        return watermarks
    finally:
        consumer.close()


def offset_json(offsets: dict[str, dict[int, int]]) -> str:
    serializable = {
        topic: {str(partition): offset for partition, offset in partitions.items()}
        for topic, partitions in offsets.items()
    }
    return json.dumps(serializable, sort_keys=True)


def select_raw_rows(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("value").cast("string").alias("_raw_payload"),
        F.current_timestamp().alias("_ingested_at"),
        F.sha2(F.col("value").cast("string"), 256).alias("_row_hash"),
        F.col("offset").cast("long").alias("_kafka_offset"),
        F.upper(F.split(F.col("topic"), "\\.").getItem(0)).alias("_source_system"),
        F.year(F.current_timestamp()).alias("_ingest_year"),
        F.month(F.current_timestamp()).alias("_ingest_month"),
        F.dayofmonth(F.current_timestamp()).alias("_ingest_day"),
    )


def select_tms_rows(df: DataFrame) -> DataFrame:
    payload = F.col("value").cast("string")
    return df.select(
        F.coalesce(F.get_json_object(payload, "$.record_id"), F.col("key").cast("string")).alias("record_id"),
        F.get_json_object(payload, "$.employee_id").alias("employee_id"),
        F.get_json_object(payload, "$.employee_name").alias("employee_name"),
        F.get_json_object(payload, "$.department").alias("department"),
        F.get_json_object(payload, "$.training_name").alias("training_name"),
        F.get_json_object(payload, "$.training_category").alias("training_category"),
        F.get_json_object(payload, "$.scheduled_date").alias("scheduled_date"),
        F.get_json_object(payload, "$.completion_date").alias("completion_date"),
        F.get_json_object(payload, "$.score").alias("score"),
        F.get_json_object(payload, "$.status").alias("status"),
        F.get_json_object(payload, "$.trainer_id").alias("trainer_id"),
        F.get_json_object(payload, "$.training_mode").alias("training_mode"),
        F.get_json_object(payload, "$.validity_months").alias("validity_months"),
        F.lit("TMS").alias("_source"),
        F.current_timestamp().alias("_ingested_at"),
    ).where(F.col("record_id").isNotNull())


def select_sap_rows(df: DataFrame) -> DataFrame:
    payload = F.col("value").cast("string")
    quantity = F.get_json_object(payload, "$.quantity").cast("int")
    return df.select(
        F.get_json_object(payload, "$.document_number").alias("po_number"),
        F.get_json_object(payload, "$.material_code").alias("material_code"),
        F.get_json_object(payload, "$.plant").alias("plant"),
        F.get_json_object(payload, "$.storage_location").alias("storage_location"),
        quantity.alias("planned_qty"),
        quantity.alias("actual_qty"),
        F.get_json_object(payload, "$.uom").alias("uom"),
        F.to_date(F.get_json_object(payload, "$.posting_date")).alias("posting_date"),
        F.get_json_object(payload, "$.movement_type").alias("cost_center"),
        F.get_json_object(payload, "$.valuation_amount_inr").cast("double").alias("total_cost"),
        F.lit("INR").alias("currency"),
        F.lit("SAP_ECC_KAFKA").alias("_source"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit("kafka_bronze_ingest").alias("_nifi_flow"),
    ).where(F.col("po_number").isNotNull())


def filter_existing_rows(spark: SparkSession, topic: str, table: str, out: DataFrame) -> DataFrame:
    try:
        existing = spark.table(table)
    except Exception:
        return out

    if topic in RAW_TOPIC_TABLES:
        keys = ["_row_hash", "_kafka_offset"]
    elif topic.startswith("tms.") or topic.startswith("cdc.tms."):
        keys = ["record_id"]
    elif topic.startswith("sap."):
        keys = ["po_number"]
    else:
        return out

    existing_keys = existing.select(*keys).dropDuplicates()
    return out.join(existing_keys, keys, "left_anti")


def append_topic(spark: SparkSession, topic: str, table: str, start: dict[int, int], end: dict[int, int]) -> int:
    if not end or all(end.get(partition, 0) <= start.get(partition, 0) for partition in end):
        print(f"No new records for {topic}")
        return 0

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", offset_json({topic: start}))
        .option("endingOffsets", offset_json({topic: end}))
        .option("failOnDataLoss", "false")
        .load()
    )

    if topic in RAW_TOPIC_TABLES:
        out = select_raw_rows(df)
    elif topic.startswith("tms.") or topic.startswith("cdc.tms."):
        out = select_tms_rows(df)
    elif topic.startswith("sap."):
        out = select_sap_rows(df)
    else:
        raise ValueError(f"No Bronze projection configured for {topic}")

    out = filter_existing_rows(spark, topic, table, out)
    rows = out.count()
    if rows:
        out.writeTo(table).append()
    print(f"Appended {rows} rows from {topic} to {table}")
    return rows


def main(args: argparse.Namespace) -> None:
    topics = list(ALL_TOPICS) if args.topic == "all" else [args.topic]
    high_watermarks = topic_high_watermarks(topics)
    state = load_state()

    if args.init_offsets:
        for topic, offsets in high_watermarks.items():
            state[topic] = {str(partition): offset for partition, offset in offsets.items()}
        save_state(state)
        print(f"Initialized Kafka Bronze offsets for {len(high_watermarks)} topics.")
        return

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        total_rows = 0
        for topic in topics:
            end = high_watermarks.get(topic)
            if not end:
                continue

            previous = state.get(topic)
            if previous is None:
                if args.bootstrap_earliest:
                    start = {partition: 0 for partition in end}
                else:
                    state[topic] = {str(partition): offset for partition, offset in end.items()}
                    print(f"Initialized {topic} at latest offsets; next run will ingest new records.")
                    continue
            else:
                start = {int(partition): int(offset) for partition, offset in previous.items()}
                for partition in end:
                    start.setdefault(partition, 0)

            rows = append_topic(spark, topic, ALL_TOPICS[topic], start, end)
            total_rows += rows
            state[topic] = {str(partition): offset for partition, offset in end.items()}

        save_state(state)
        print(f"Kafka Bronze ingestion complete. Rows appended: {total_rows}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append new Kafka records into existing Iceberg Bronze tables.")
    parser.add_argument("--topic", default="all", help="Kafka topic to ingest, or 'all'.")
    parser.add_argument("--init-offsets", action="store_true", help="Record current high watermarks without ingesting.")
    parser.add_argument("--bootstrap-earliest", action="store_true", help="For topics without saved offsets, ingest from offset 0.")
    main(parser.parse_args())
