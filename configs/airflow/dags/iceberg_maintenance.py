"""
Daily maintenance: expire snapshots, rewrite data/manifest files,
analyze tables, push metrics to Prometheus.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
WAREHOUSE = "s3a://lakehouse-bronze/warehouse"
SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]
)

BRONZE_TABLES = [
    "lakehouse.bronze.mes_production_orders",
    "lakehouse.bronze.iqms_quality_tests",
    "lakehouse.bronze.iqms_deviations",
    "lakehouse.bronze.sap_ecc_orders",
    "lakehouse.bronze.trackwise_capas",
    "lakehouse.bronze.tms_training_completions",
]

SILVER_TABLES = [
    "lakehouse.silver.silver_mes_production_orders",
    "lakehouse.silver.silver_iqms_quality_tests",
    "lakehouse.silver.silver_iqms_deviations",
    "lakehouse.silver.silver_sap_inventory",
    "lakehouse.silver.silver_trackwise_capas",
    "lakehouse.silver.silver_tms_training",
]

GOLD_TABLES = [
    "lakehouse.gold.gold_manufacturing_oee_mart",
    "lakehouse.gold.gold_compliance_capa_mart",
    "lakehouse.gold.gold_sap_inventory_mart",
    "lakehouse.gold.gold_quality_risk_mart",
    "lakehouse.gold.gold_training_compliance_mart",
]

ALL_TABLES = BRONZE_TABLES + SILVER_TABLES + GOLD_TABLES
EXPIRE_SNAPSHOT_TABLES = [
    table
    for table in ALL_TABLES
    if table != "lakehouse.bronze.sap_ecc_orders"
]

SPARK_CONF = {
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.lakehouse.warehouse": WAREHOUSE,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs-s3:8333",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "admin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="iceberg_maintenance",
    default_args=default_args,
    description="Daily Iceberg table maintenance: snapshots, compaction, analysis",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "maintenance", "compaction"],
)


def run_iceberg_maintenance(**context):
    """
    Run Iceberg maintenance tasks via SparkSQL:
    1. Expire old snapshots (>7 days)
    2. Rewrite data files (compaction) for silver tables
    3. Rewrite manifests for gold tables
    4. ANALYZE TABLE for statistics refresh
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("iceberg_maintenance")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.catalog.lakehouse", SPARK_CONF["spark.sql.catalog.lakehouse"])
        .config("spark.sql.catalog.lakehouse.type", SPARK_CONF["spark.sql.catalog.lakehouse.type"])
        .config("spark.sql.catalog.lakehouse.uri", SPARK_CONF["spark.sql.catalog.lakehouse.uri"])
        .config("spark.sql.catalog.lakehouse.warehouse", SPARK_CONF["spark.sql.catalog.lakehouse.warehouse"])
        .config("spark.sql.extensions", SPARK_CONF["spark.sql.extensions"])
        .config("spark.hadoop.fs.s3a.endpoint", SPARK_CONF["spark.hadoop.fs.s3a.endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", SPARK_CONF["spark.hadoop.fs.s3a.access.key"])
        .config("spark.hadoop.fs.s3a.secret.key", SPARK_CONF["spark.hadoop.fs.s3a.secret.key"])
        .config("spark.hadoop.fs.s3a.path.style.access", SPARK_CONF["spark.hadoop.fs.s3a.path.style.access"])
        .config("spark.hadoop.fs.s3a.impl", SPARK_CONF["spark.hadoop.fs.s3a.impl"])
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", SPARK_CONF["spark.hadoop.fs.s3a.connection.ssl.enabled"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    results = {"expired": 0, "compacted": 0, "rewritten": 0, "analyzed": 0, "errors": []}

    for table in EXPIRE_SNAPSHOT_TABLES:
        try:
            spark.sql(f"""
                CALL lakehouse.system.expire_snapshots(
                    table => '{table}',
                    older_than => TIMESTAMP '{(datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')}',
                    retain_last => 5
                )
            """)
            results["expired"] += 1
            print(f"  ✅ Expired snapshots: {table}")
        except Exception as e:
            results["errors"].append(f"expire_snapshots({table}): {e}")
            print(f"  ⚠️ Expire failed: {table}: {e}")

    for table in SILVER_TABLES:
        try:
            spark.sql(f"""
                CALL lakehouse.system.rewrite_data_files(
                    table => '{table}'
                )
            """)
            results["compacted"] += 1
            print(f"  ✅ Compacted: {table}")
        except Exception as e:
            results["errors"].append(f"rewrite_data_files({table}): {e}")
            print(f"  ⚠️ Compaction failed: {table}: {e}")

    for table in GOLD_TABLES:
        try:
            spark.sql(f"""
                CALL lakehouse.system.rewrite_manifests(
                    table => '{table}'
                )
            """)
            results["rewritten"] += 1
            print(f"  ✅ Rewritten manifests: {table}")
        except Exception as e:
            results["errors"].append(f"rewrite_manifests({table}): {e}")
            print(f"  ⚠️ Manifest rewrite failed: {table}: {e}")

    for table in ALL_TABLES:
        try:
            spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")
            results["analyzed"] += 1
            print(f"  ✅ Analyzed: {table}")
        except Exception as e:
            print(f"  ⚠️ Analyze skipped: {table}: {e}")

    spark.stop()

    print(f"\n{'='*60}")
    print(f"Iceberg Maintenance Summary:")
    print(f"  Snapshots expired: {results['expired']}/{len(EXPIRE_SNAPSHOT_TABLES)}")
    print(f"  Silver compacted:  {results['compacted']}/{len(SILVER_TABLES)}")
    print(f"  Gold manifests:    {results['rewritten']}/{len(GOLD_TABLES)}")
    print(f"  Tables analyzed:   {results['analyzed']}/{len(ALL_TABLES)}")
    print(f"  Errors:            {len(results['errors'])}")

    context["ti"].xcom_push(key="maintenance_results", value=results)

    if results["errors"]:
        print(f"\nErrors:")
        for err in results["errors"]:
            print(f"  ❌ {err}")


def push_maintenance_metrics(**context):
    """Push maintenance metrics to Prometheus Pushgateway (if available)."""
    ti = context["ti"]
    results = ti.xcom_pull(key="maintenance_results", task_ids="run_maintenance")

    if not results:
        return

    try:
        import httpx
        metrics = (
            f'# HELP iceberg_maintenance_snapshots_expired Count of tables with expired snapshots\n'
            f'# TYPE iceberg_maintenance_snapshots_expired gauge\n'
            f'iceberg_maintenance_snapshots_expired {results.get("expired", 0)}\n'
            f'# HELP iceberg_maintenance_tables_compacted Count of tables compacted\n'
            f'# TYPE iceberg_maintenance_tables_compacted gauge\n'
            f'iceberg_maintenance_tables_compacted {results.get("compacted", 0)}\n'
            f'# HELP iceberg_maintenance_errors Count of maintenance errors\n'
            f'# TYPE iceberg_maintenance_errors gauge\n'
            f'iceberg_maintenance_errors {len(results.get("errors", []))}\n'
        )
        response = httpx.post(
            f"{PUSHGATEWAY_URL}/metrics/job/iceberg_maintenance",
            content=metrics,
            headers={"Content-Type": "text/plain"},
            timeout=10.0,
        )
        print(f"Metrics pushed to Pushgateway: {response.status_code}")
    except Exception as e:
        print(f"Could not push metrics (Pushgateway may not be available): {e}")


t_maintenance = PythonOperator(
    task_id="run_maintenance",
    python_callable=run_iceberg_maintenance,
    dag=dag,
)

t_metrics = PythonOperator(
    task_id="push_metrics",
    python_callable=push_maintenance_metrics,
    dag=dag,
)

t_maintenance >> t_metrics
