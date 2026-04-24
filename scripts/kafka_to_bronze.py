from __future__ import annotations

import sys
from pyspark.sql import SparkSession, functions as F

KAFKA_SERVERS = "kafka:9092"
ICEBERG_BASE = "s3a://lakehouse-bronze/warehouse"

# Mapping datagen topics to Bronze tables
TOPIC_TO_TABLE = {
    "mes.production_orders": "lakehouse.bronze.mes_production_orders",
    "iqms.quality_tests": "lakehouse.bronze.iqms_quality_tests",
    "tms.training_completions": "lakehouse.bronze.tms_training_completions",
}

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("kafka-to-bronze-recovery")
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

def main() -> None:
    spark = build_spark()
    
    # Enable Iceberg support for creating tables if they don't exist
    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze
        LOCATION '{ICEBERG_BASE}/bronze.db'
        """
    )

    for topic, table in TOPIC_TO_TABLE.items():
        print(f"\n>>> PROCESSING TOPIC: {topic} -> {table}")
        
        try:
            # Read from Kafka (batch mode to ingest what's currently there)
            df = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_SERVERS)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
            )

            if df.count() == 0:
                print(f"!!! No messages found in topic {topic}. Skipping.")
                continue

            print(f"--- Found {df.count()} messages. Ingesting...")

            # Transform to Bronze schema
            processed_df = (
                df.select(
                    F.col("key").cast("string").alias("_kafka_key"),
                    F.col("value").cast("string").alias("_raw_payload"),
                    F.col("topic").alias("_kafka_topic"),
                    F.col("partition").alias("_kafka_partition"),
                    F.col("offset").alias("_kafka_offset"),
                    F.col("timestamp").alias("_kafka_timestamp")
                )
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_system", F.lit(topic.split('.')[0].upper()))
                .withColumn("_row_hash", F.sha2(F.col("_raw_payload"), 256))
                .withColumn("_ingest_year", F.year(F.col("_ingested_at")))
                .withColumn("_ingest_month", F.month(F.col("_ingested_at")))
                .withColumn("_ingest_day", F.dayofmonth(F.col("_ingested_at")))
                .withColumn("_ingest_hour", F.hour(F.col("_ingested_at")))
            )

            # Write to Iceberg
            # We use append() to add to existing data (like those 3 rows)
            processed_df.writeTo(table).append()
            print(f"+++ Success: Ingested {topic} into {table}")

        except Exception as e:
            print(f"XXX Error processing {topic}: {e}")

    print("\nBulk Ingestion Complete.")
    spark.stop()

if __name__ == "__main__":
    main()
