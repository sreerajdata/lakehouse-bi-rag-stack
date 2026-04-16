"""
TPL Data Lakehouse - Master Medallion Pipeline DAG
Orchestrates: Kafka → Bronze → Silver → Gold via Spark + dbt
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
import logging

log = logging.getLogger(__name__)

# ── DAG Defaults ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── S3 / SeaweedFS Paths ──────────────────────────────────────────────────────
S3_ENDPOINT   = "http://seaweedfs-s3:8333"
BRONZE_PATH   = "s3a://lakehouse-bronze"
SILVER_PATH   = "s3a://lakehouse-silver"
GOLD_PATH     = "s3a://lakehouse-gold"

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "admin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.lakehouse.warehouse": f"{GOLD_PATH}/warehouse",
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="tpl_medallion_pipeline",
    description="Full Bronze → Silver → Gold Medallion Pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",   # every hour
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "tpl"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Bronze Layer: Kafka → Raw Iceberg Tables ──────────────────────────────
    with TaskGroup("bronze_ingestion", tooltip="Kafka → Bronze Iceberg") as bronze_group:

        bronze_mes = SparkSubmitOperator(
            task_id="bronze_mes",
            application="/opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py",
            conf=SPARK_CONF,
            application_args=["--topic", "mes.production_orders",
                               "--table", "lakehouse.bronze.mes_production_orders",
                               "--path", f"{BRONZE_PATH}/mes/production_orders"],
            packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.4",
        )

        bronze_mes_status = SparkSubmitOperator(
            task_id="bronze_mes_machine_status",
            application="/opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py",
            conf=SPARK_CONF,
            application_args=["--topic", "mes.machine_status",
                               "--table", "lakehouse.bronze.mes_machine_status",
                               "--path", f"{BRONZE_PATH}/mes/machine_status"],
            packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.4",
        )

        bronze_iqms = SparkSubmitOperator(
            task_id="bronze_iqms",
            application="/opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py",
            conf=SPARK_CONF,
            application_args=["--topic", "iqms.quality_tests",
                               "--table", "lakehouse.bronze.iqms_quality_tests",
                               "--path", f"{BRONZE_PATH}/iqms/quality_tests"],
            packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.4",
        )

        bronze_historian = SparkSubmitOperator(
            task_id="bronze_historian",
            application="/opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py",
            conf=SPARK_CONF,
            application_args=["--topic", "historian.process_parameters",
                               "--table", "lakehouse.bronze.historian_process_params",
                               "--path", f"{BRONZE_PATH}/historian/process_params"],
            packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.4",
        )

        bronze_sap = SparkSubmitOperator(
            task_id="bronze_sap",
            application="/opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py",
            conf=SPARK_CONF,
            application_args=["--topic", "sap.inventory_movements",
                               "--table", "lakehouse.bronze.sap_inventory_movements",
                               "--path", f"{BRONZE_PATH}/sap/inventory_movements"],
            packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.4",
        )

        bronze_trackwise = SparkSubmitOperator(
            task_id="bronze_trackwise",
            application="/opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py",
            conf=SPARK_CONF,
            application_args=["--topic", "trackwise.capas",
                               "--table", "lakehouse.bronze.trackwise_capas",
                               "--path", f"{BRONZE_PATH}/trackwise/capas"],
            packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.4",
        )

        bronze_tms = SparkSubmitOperator(
            task_id="bronze_tms",
            application="/opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py",
            conf=SPARK_CONF,
            application_args=["--topic", "tms.training_completions",
                               "--table", "lakehouse.bronze.tms_training_completions",
                               "--path", f"{BRONZE_PATH}/tms/training_completions"],
            packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-aws:3.3.4",
        )

    # ── Silver Layer: dbt Cleaning & Enrichment ───────────────────────────────
    with TaskGroup("silver_transform", tooltip="Bronze → Silver via dbt") as silver_group:

        dbt_silver = BashOperator(
            task_id="dbt_run_silver",
            bash_command="""
                cd /usr/app/dbt && \
                dbt run --select silver --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
            """,
            env={"DBT_PROFILES_DIR": "/usr/app/dbt"},
        )

        dbt_test_silver = BashOperator(
            task_id="dbt_test_silver",
            bash_command="""
                cd /usr/app/dbt && \
                dbt test --select silver --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
            """,
        )

        dbt_silver >> dbt_test_silver

    # ── Silver Layer: Great Expectations Quality Gates ────────────────────────
    with TaskGroup("quality_gates", tooltip="Great Expectations DQ checks") as dq_group:

        def run_ge_checkpoint(checkpoint_name: str, **kwargs):
            """Run a Great Expectations checkpoint."""
            import subprocess
            result = subprocess.run(
                ["python", "/opt/airflow/dags/ge_checkpoints/run_checkpoint.py",
                 "--checkpoint", checkpoint_name],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                raise ValueError(f"GE checkpoint {checkpoint_name} failed:\n{result.stderr}")
            log.info(result.stdout)

        ge_mes = PythonOperator(
            task_id="ge_check_mes",
            python_callable=run_ge_checkpoint,
            op_kwargs={"checkpoint_name": "mes_silver_checkpoint"},
        )
        ge_iqms = PythonOperator(
            task_id="ge_check_iqms",
            python_callable=run_ge_checkpoint,
            op_kwargs={"checkpoint_name": "iqms_silver_checkpoint"},
        )
        ge_sap = PythonOperator(
            task_id="ge_check_sap",
            python_callable=run_ge_checkpoint,
            op_kwargs={"checkpoint_name": "sap_silver_checkpoint"},
        )

    # ── Gold Layer: dbt Domain Data Marts ─────────────────────────────────────
    with TaskGroup("gold_marts", tooltip="Silver → Gold Domain Marts") as gold_group:

        dbt_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command="""
                cd /usr/app/dbt && \
                dbt run --select gold --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
            """,
        )

        dbt_test_gold = BashOperator(
            task_id="dbt_test_gold",
            bash_command="""
                cd /usr/app/dbt && \
                dbt test --select gold --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
            """,
        )

        dbt_docs = BashOperator(
            task_id="dbt_generate_docs",
            bash_command="""
                cd /usr/app/dbt && \
                dbt docs generate --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
            """,
        )

        dbt_gold >> dbt_test_gold >> dbt_docs

    # ── DataHub Lineage Emission ───────────────────────────────────────────────
    emit_lineage = BashOperator(
        task_id="emit_datahub_lineage",
        bash_command="""
            python /opt/airflow/dags/datahub_lineage/emit_lineage.py \
                --gms-url http://datahub-gms:8080 \
                --run-id {{ run_id }}
        """,
    )

    # ── Pipeline Wiring ───────────────────────────────────────────────────────
    (
        start
        >> bronze_group
        >> silver_group
        >> dq_group
        >> gold_group
        >> emit_lineage
        >> end
    )
