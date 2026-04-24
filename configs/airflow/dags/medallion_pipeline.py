from __future__ import annotations

import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import os
from typing import Optional

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from sqlalchemy import create_engine, text

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient


ROOT_DIR = Path("/opt/airflow")
SCRIPTS_DIR = ROOT_DIR / "scripts"
DBT_DIR = ROOT_DIR / "dbt"
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_MAP = {
    "mes_events": "mes.production_orders",
    "iqms_orders": "iqms.quality_tests",
    "trackwise_deviations": "iqms.deviations",
    "sap_ecc_orders": "sap.inventory_movements",
    "sop_documents": "tms.training_completions",
}
TRINO_SQLALCHEMY_URL = "trino://admin@trino:8080/iceberg"
SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]
)
SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.submit.deployMode": "client",
    "spark.pyspark.driver.python": "/usr/local/bin/python",
    "spark.pyspark.python": "/opt/bitnami/python/bin/python3",
    "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs-s3:8333",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "admin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse-bronze/warehouse",
}


def run_python_script(script_name: str, extra_env: Optional[dict] = None) -> str:
    script_path = SCRIPTS_DIR / script_name
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(
        ["python", str(script_path)],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        env=env,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")
    return result.stdout


def generate_source_data_task():
    print("Skipping generation (handled by external synthetic_datagen container)")
    return "Skipped"

def publish_to_kafka_task():
    print("Skipping publish (handled by external synthetic_datagen container)")
    return "Skipped"


def check_kafka_topics_task():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    metadata = admin.list_topics(timeout=15)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "airflow-topic-check",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    counts = {}
    try:
        for topic in TOPIC_MAP.values():
            if topic not in metadata.topics:
                raise RuntimeError(f"Kafka topic {topic} does not exist")
            topic_meta = metadata.topics[topic]
            if topic_meta.error is not None:
                raise RuntimeError(f"Kafka topic {topic} metadata error: {topic_meta.error}")

            topic_count = 0
            for partition_id in topic_meta.partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10)
                topic_count += max(high - low, 0)

            print(f"{topic}: {topic_count} messages")
            if topic_count <= 0:
                raise RuntimeError(f"Kafka topic {topic} has no messages")
            counts[topic] = topic_count
    finally:
        consumer.close()
    return counts


def run_gx_validation_task():
    output = run_python_script("run_gx_validation.py")
    matches = re.findall(r"Checkpoint .*?: (\d+)/(\d+) expectations passed", output)
    passed = sum(int(match[0]) for match in matches)
    total = sum(int(match[1]) for match in matches)
    return {"passed": passed, "total": total}


def query_scalar(sql: str) -> int:
    engine = create_engine(TRINO_SQLALCHEMY_URL)
    with engine.connect() as connection:
        value = connection.execute(text(sql)).scalar()
    return int(value or 0)


def notify_pipeline_complete_task(ti=None, **_kwargs):
    bronze_rows = sum(
        query_scalar(f"select count(*) from iceberg.bronze.{table}")
        for table in [
            "mes_events",
            "iqms_orders",
            "trackwise_deviations",
            "sap_ecc_orders",
            "sop_documents",
        ]
    )
    silver_rows = sum(
        query_scalar(f"select count(*) from iceberg.silver.{table}")
        for table in [
            "silver_mes_events",
            "silver_quality_events",
            "silver_production_orders",
        ]
    )
    gold_rows = sum(
        query_scalar(f"select count(*) from iceberg.gold.{table}")
        for table in [
            "gold_oee_dashboard",
            "gold_batch_summary",
            "gold_quality_kpis",
            "gold_production_efficiency",
        ]
    )
    gx_summary = ti.xcom_pull(task_ids="run_gx_validation") or {"passed": 0, "total": 0}
    start = ti.dag_run.start_date
    end = datetime.utcnow()
    duration_minutes = round((end - start.replace(tzinfo=None)).total_seconds() / 60.0, 2) if start else 0
    summary = (
        f"Pipeline complete: {bronze_rows} bronze rows -> {silver_rows} silver rows -> {gold_rows} gold rows\n"
        f"Quality: {gx_summary['passed']}/{gx_summary['total']} GX expectations passed\n"
        f"Duration: {duration_minutes} minutes"
    )
    print(summary)
    return summary


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="medallion_full_pipeline",
    description="End-to-end lakehouse demo pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "demo", "medallion"],
) as dag:
    generate_source_data = PythonOperator(
        task_id="generate_source_data",
        python_callable=generate_source_data_task,
    )

    publish_to_kafka = PythonOperator(
        task_id="publish_to_kafka",
        python_callable=publish_to_kafka_task,
    )

    check_kafka_topics = PythonOperator(
        task_id="check_kafka_topics",
        python_callable=check_kafka_topics_task,
    )

    spark_bronze_ingest = SparkSubmitOperator(
        task_id="spark_bronze_ingest",
        conn_id="spark_lakehouse",
        application=str(SCRIPTS_DIR / "create_bronze_tables.py"),
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
        verbose=True,
    )

    verify_bronze_counts = TrinoOperator(
        task_id="verify_bronze_counts",
        trino_conn_id="trino_lakehouse",
        sql=(SCRIPTS_DIR / "verify_bronze.sql").read_text(encoding="utf-8"),
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && dbt run --select silver --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_DIR} && dbt test --select silver --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    run_gx_validation = PythonOperator(
        task_id="run_gx_validation",
        python_callable=run_gx_validation_task,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && dbt run --select gold --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_DIR} && dbt test --select gold --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    notify_pipeline_complete = PythonOperator(
        task_id="notify_pipeline_complete",
        python_callable=notify_pipeline_complete_task,
    )

    (
        generate_source_data
        >> publish_to_kafka
        >> check_kafka_topics
        >> spark_bronze_ingest
        >> verify_bronze_counts
        >> dbt_run_silver
        >> dbt_test_silver
        >> run_gx_validation
        >> dbt_run_gold
        >> dbt_test_gold
        >> notify_pipeline_complete
    )
