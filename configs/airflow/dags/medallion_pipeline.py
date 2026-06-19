from __future__ import annotations

import json
import re
import shutil
import subprocess
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
import os
from typing import Optional

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from sqlalchemy import create_engine, text

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient


ROOT_DIR = Path("/opt/airflow")
SCRIPTS_DIR = ROOT_DIR / "scripts"
DBT_DIR = ROOT_DIR / "dbt"
DAGS_DIR = ROOT_DIR / "dags"
DATAHUB_CONFIG_DIR = ROOT_DIR / "configs" / "datahub"
AIRFLOW_LOCAL_BIN = Path("/home/airflow/.local/bin")
SOURCE_DIR = ROOT_DIR / "data" / "source"
BRONZE_MANIFEST_PATH = ROOT_DIR / "data" / ".bronze_source_manifest.json"
PRIMARY_SOURCE_FILES = [
    "mes_events.csv",
    "iqms_orders.csv",
    "trackwise_deviations.csv",
    "sap_ecc_orders.csv",
    "sop_documents.csv",
]
KAFKA_BOOTSTRAP = "kafka:9092"
NIFI_API_URL = os.getenv("NIFI_API_URL", "http://nifi:8090/nifi-api")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://datahub-gms:8080")
DATAHUB_REQUIRED = os.getenv("DATAHUB_REQUIRED", "false").lower() == "true"
TOPIC_MAP = {
    "mes_events": "mes.production_orders",
    "iqms_orders": "iqms.quality_tests",
    "trackwise_deviations": "iqms.deviations",
    "sap_ecc_orders": "sap.inventory_movements",
    "sop_documents": "tms.training_completions",
}
SCHEMA_REGISTRY_TOPICS = sorted(
    {
        *TOPIC_MAP.values(),
        "mes.machine_status",
        "mes.oee_metrics",
        "historian.process_parameters",
        "trackwise.capas",
        "trackwise.complaints",
        "sap.purchase_orders",
    }
)
TRINO_SQLALCHEMY_URL = "trino://admin@trino:8080/iceberg"
SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]
)
KAFKA_SPARK_PACKAGES = ",".join(
    [
        SPARK_PACKAGES,
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3",
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


def build_source_manifest() -> dict[str, dict[str, int]]:
    manifest = {}
    for filename in PRIMARY_SOURCE_FILES:
        path = SOURCE_DIR / filename
        if not path.exists():
            continue
        stat = path.stat()
        manifest[filename] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    return manifest


def load_previous_manifest() -> dict[str, dict[str, int]]:
    if not BRONZE_MANIFEST_PATH.exists():
        return {}
    return json.loads(BRONZE_MANIFEST_PATH.read_text(encoding="utf-8"))


def deploy_nifi_flows_task() -> str:
    """
    Deploy NiFi flows via the existing create_nifi_flow.py script.
    The script is idempotent: if FLOW 1 and FLOW 2 already exist it will
    start them; if missing it will create, configure, and start them.
    Runs against the NiFi REST API at http://nifi:8090/nifi-api which is
    reachable from the Airflow worker on lakehouse_net.
    """
    env = {
        "NIFI_BASE_URL": "http://nifi:8090/nifi-api",
        "NIFI_CLEANUP_EXISTING": "false",
    }
    output = run_python_script("create_nifi_flow.py", extra_env=env)
    print(output)
    return "NiFi flows deployed / verified"


def generate_source_data_task():
    print("Skipping generation (handled by external synthetic_datagen container)")
    return "Skipped"

def publish_to_kafka_task():
    print("Skipping publish (handled by external synthetic_datagen container)")
    return "Skipped"


def run_bronze_ingest_task():
    current_manifest = build_source_manifest()
    previous_manifest = load_previous_manifest()
    if current_manifest and current_manifest == previous_manifest:
        print("Skipping Bronze Spark sync because source files are unchanged.")
        return "Skipped"

    result = subprocess.run(
        [
            "spark-submit",
            "--packages",
            SPARK_PACKAGES,
            str(SCRIPTS_DIR / "create_bronze_tables.py"),
        ],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"spark bronze ingest failed with exit code {result.returncode}")
    return "Completed"


def run_kafka_bronze_ingest_task():
    result = subprocess.run(
        [
            "spark-submit",
            "--packages",
            KAFKA_SPARK_PACKAGES,
            str(DAGS_DIR / "spark_jobs" / "bronze_kafka_to_iceberg.py"),
        ],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"kafka bronze ingest failed with exit code {result.returncode}")
    return "Completed"


def register_schema_subjects_task():
    schema = {
        "type": "object",
        "additionalProperties": True,
    }
    registered = []
    for topic in SCHEMA_REGISTRY_TOPICS:
        payload = json.dumps(
            {
                "schemaType": "JSON",
                "schema": json.dumps(schema),
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions",
            data=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            registered.append({"topic": topic, "status": response.status})
    print(f"Registered/verified {len(registered)} Schema Registry subjects.")
    return registered


def validate_nifi_integration_task():
    with urllib.request.urlopen(f"{NIFI_API_URL}/flow/process-groups/root", timeout=20) as response:
        flow = json.loads(response.read().decode("utf-8"))

    process_groups = {
        group["component"]["name"]
        for group in flow["processGroupFlow"]["flow"].get("processGroups", [])
    }
    required_groups = {"FLOW 1 - CSV File Ingestion", "FLOW 2 - Kafka Consumer Flow"}
    missing_groups = sorted(required_groups - process_groups)
    if missing_groups:
        raise RuntimeError(f"NiFi process groups missing: {missing_groups}")

    nifi_rows = query_scalar(
        """
        select count(*)
        from (
            select _nifi_flow from iceberg.bronze.mes_events
            union all select _nifi_flow from iceberg.bronze.iqms_orders
            union all select _nifi_flow from iceberg.bronze.trackwise_deviations
            union all select _nifi_flow from iceberg.bronze.sap_ecc_orders
        ) as nifi_sources
        where _nifi_flow = 'nifi_s3_ingest'
        """
    )
    if nifi_rows <= 0:
        raise RuntimeError("NiFi is configured, but no Bronze rows are stamped with _nifi_flow='nifi_s3_ingest'")

    print(f"NiFi integration verified: {nifi_rows} Bronze rows came through nifi_s3_ingest.")
    return {"nifi_bronze_rows": nifi_rows}


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


def emit_datahub_lineage_task(ti=None, **_kwargs):
    run_id = ti.dag_run.run_id if ti and ti.dag_run else "manual"
    script_path = DAGS_DIR / "datahub_lineage" / "emit_lineage.py"
    result = subprocess.run(
        [
            "python",
            str(script_path),
            "--gms-url",
            DATAHUB_GMS_URL,
            "--run-id",
            run_id,
        ],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        message = f"DataHub lineage emission failed with exit code {result.returncode}"
        if DATAHUB_REQUIRED:
            raise RuntimeError(message)
        print(f"{message}; continuing because DATAHUB_REQUIRED is false.")
        return "Skipped DataHub lineage emission"
    return "Completed"


def run_datahub_ingestion_task() -> str:
    recipes = [
        DATAHUB_CONFIG_DIR / "recipe_trino.yml",
        DATAHUB_CONFIG_DIR / "recipe_dbt.yml",
    ]
    env = os.environ.copy()
    env["PATH"] = f"{AIRFLOW_LOCAL_BIN}:{env.get('PATH', '')}"
    datahub_cli = shutil.which("datahub", path=env["PATH"])
    if not datahub_cli:
        message = (
            "DataHub CLI was not found. Rebuild the Airflow image so "
            "acryl-datahub is installed, or set DATAHUB_REQUIRED=false to keep "
            "governance ingestion best-effort."
        )
        if DATAHUB_REQUIRED:
            raise RuntimeError(message)
        print(f"{message} Continuing without DataHub ingestion.")
        return "Skipped DataHub ingestion"

    completed = []
    for recipe in recipes:
        result = subprocess.run(
            [datahub_cli, "ingest", "-c", str(recipe)],
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
            message = f"DataHub ingestion failed for {recipe.name} with exit code {result.returncode}"
            if DATAHUB_REQUIRED:
                raise RuntimeError(message)
            print(f"{message}; continuing because DATAHUB_REQUIRED is false.")
            return f"Skipped remaining DataHub ingestion after {recipe.name} failed"
        completed.append(recipe.name)
    return f"Completed DataHub ingestion: {', '.join(completed)}"


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
    
    try:
        pushgateway_url = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
        metrics = (
            f'# HELP medallion_bronze_rows Total rows in bronze\n'
            f'# TYPE medallion_bronze_rows gauge\n'
            f'medallion_bronze_rows {bronze_rows}\n'
            f'# HELP medallion_silver_rows Total rows in silver\n'
            f'# TYPE medallion_silver_rows gauge\n'
            f'medallion_silver_rows {silver_rows}\n'
            f'# HELP medallion_gold_rows Total rows in gold\n'
            f'# TYPE medallion_gold_rows gauge\n'
            f'medallion_gold_rows {gold_rows}\n'
            f'# HELP medallion_gx_passed GX expectations passed\n'
            f'# TYPE medallion_gx_passed gauge\n'
            f'medallion_gx_passed {gx_summary.get("passed", 0)}\n'
            f'# HELP medallion_gx_total GX expectations total\n'
            f'# TYPE medallion_gx_total gauge\n'
            f'medallion_gx_total {gx_summary.get("total", 0)}\n'
            f'# HELP medallion_duration_minutes Pipeline duration\n'
            f'# TYPE medallion_duration_minutes gauge\n'
            f'medallion_duration_minutes {duration_minutes}\n'
        )
        request = urllib.request.Request(
            f"{pushgateway_url}/metrics/job/medallion_pipeline",
            data=metrics.encode("utf-8"),
            headers={"Content-Type": "text/plain; version=0.0.4"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            print(f"Metrics pushed to Pushgateway: {response.status}")
    except Exception as e:
        print(f"Could not push metrics (Pushgateway may not be available): {e}")

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
    deploy_nifi_flows = PythonOperator(
        task_id="deploy_nifi_flows",
        python_callable=deploy_nifi_flows_task,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

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

    register_schema_subjects = PythonOperator(
        task_id="register_schema_subjects",
        python_callable=register_schema_subjects_task,
        do_xcom_push=False,
    )

    spark_bronze_ingest = PythonOperator(
        task_id="spark_bronze_ingest",
        python_callable=run_bronze_ingest_task,
    )

    validate_nifi_integration = PythonOperator(
        task_id="validate_nifi_integration",
        python_callable=validate_nifi_integration_task,
        do_xcom_push=False,
    )

    kafka_bronze_ingest = PythonOperator(
        task_id="kafka_bronze_ingest",
        python_callable=run_kafka_bronze_ingest_task,
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

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_DIR} && dbt docs generate --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    ingest_datahub_metadata = PythonOperator(
        task_id="ingest_datahub_metadata",
        python_callable=run_datahub_ingestion_task,
        do_xcom_push=False,
    )

    emit_datahub_lineage = PythonOperator(
        task_id="emit_datahub_lineage",
        python_callable=emit_datahub_lineage_task,
        do_xcom_push=False,
    )

    trigger_iceberg_maintenance = TriggerDagRunOperator(
        task_id="trigger_iceberg_maintenance",
        trigger_dag_id="iceberg_maintenance",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    notify_pipeline_complete = PythonOperator(
        task_id="notify_pipeline_complete",
        python_callable=notify_pipeline_complete_task,
    )

    (
        deploy_nifi_flows
        >> generate_source_data
        >> publish_to_kafka
        >> register_schema_subjects
        >> check_kafka_topics
        >> spark_bronze_ingest
        >> validate_nifi_integration
        >> kafka_bronze_ingest
        >> verify_bronze_counts
        >> dbt_run_silver
        >> dbt_test_silver
        >> run_gx_validation
        >> dbt_run_gold
        >> dbt_test_gold
        >> dbt_docs_generate
        >> ingest_datahub_metadata
        >> emit_datahub_lineage
        >> trigger_iceberg_maintenance
        >> notify_pipeline_complete
    )
