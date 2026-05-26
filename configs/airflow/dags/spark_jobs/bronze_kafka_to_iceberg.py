"""
Bronze Layer Spark Job: Kafka Topic → Apache Iceberg Table
Handles schema evolution, partitioning, and ALCOA+ metadata tagging.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    to_timestamp, year, month, dayofmonth, hour
)
from pyspark.sql.types import StringType, StructType, StructField

KAFKA_SERVERS = "kafka:9092"


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )


def ingest_kafka_to_bronze(topic: str, table: str, output_path: str):
    """
    Micro-batch read from Kafka → write to Iceberg bronze table.
    Adds ALCOA+ metadata: _source_system, _ingested_at, _batch_id, _row_hash
    """
    spark = get_spark(f"bronze_ingest_{topic.replace('.', '_')}")
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )

    raw_df = kafka_df.select(
        col("key").cast(StringType()).alias("_kafka_key"),
        col("value").cast(StringType()).alias("_raw_payload"),
        col("topic").alias("_kafka_topic"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("timestamp").alias("_kafka_timestamp"),
    )

    import hashlib
    from pyspark.sql.functions import sha2, concat_ws, md5

    enriched_df = raw_df.withColumn(
        "_ingested_at", current_timestamp()
    ).withColumn(
        "_source_system", lit(topic.split(".")[0].upper())
    ).withColumn(
        "_row_hash", sha2(col("_raw_payload"), 256)
    ).withColumn(
        "_ingest_year",  year(current_timestamp())
    ).withColumn(
        "_ingest_month", month(current_timestamp())
    ).withColumn(
        "_ingest_day",   dayofmonth(current_timestamp())
    ).withColumn(
        "_ingest_hour",  hour(current_timestamp())
    )

    def write_batch(batch_df, batch_id):
        table_parts = table.split(".")
        namespace = ".".join(table_parts[:-1])
        table_name = table_parts[-1]

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                _kafka_key        STRING,
                _raw_payload      STRING,
                _kafka_topic      STRING,
                _kafka_partition  INT,
                _kafka_offset     BIGINT,
                _kafka_timestamp  TIMESTAMP,
                _ingested_at      TIMESTAMP,
                _source_system    STRING,
                _row_hash         STRING,
                _ingest_year      INT,
                _ingest_month     INT,
                _ingest_day       INT,
                _ingest_hour      INT
            )
            USING iceberg
            PARTITIONED BY (_ingest_year, _ingest_month, _ingest_day)
            LOCATION '{output_path}'
            TBLPROPERTIES (
                'write.format.default'         = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.metadata.compression-codec' = 'gzip',
                'history.expire.max-snapshot-age-ms' = '604800000'
            )
        """)

        batch_df.writeTo(table).append()
        print(f"[batch_id={batch_id}] Written {batch_df.count()} rows to {table}")

    query = (
        enriched_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{output_path}/_checkpoint")
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination(timeout=600)
    query.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic",  required=True)
    parser.add_argument("--table",  required=True)
    parser.add_argument("--path",   required=True)
    args = parser.parse_args()

    ingest_kafka_to_bronze(args.topic, args.table, args.path)
