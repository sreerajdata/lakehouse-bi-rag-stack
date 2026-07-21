$ErrorActionPreference = "Stop"

function Step($n, $msg) {
    Write-Host ""
    Write-Host "=================================================" -ForegroundColor Cyan
    Write-Host "  STREAMING STEP $n : $msg" -ForegroundColor Cyan
    Write-Host "=================================================" -ForegroundColor Cyan
}

function Trino($sql) {
    docker exec lakehouse_trino trino --server http://localhost:8080 --execute $sql
}

$jars = @(
    "org.apache.spark_spark-sql-kafka-0-10_2.12-3.4.1.jar",
    "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.4.1.jar",
    "org.apache.kafka_kafka-clients-3.3.2.jar",
    "org.apache.commons_commons-pool2-2.11.1.jar",
    "org.apache.iceberg_iceberg-spark-runtime-3.4_2.12-1.4.3.jar",
    "org.apache.hadoop_hadoop-aws-3.3.4.jar",
    "com.amazonaws_aws-java-sdk-bundle-1.12.262.jar"
)

$jarDir = Join-Path $env:TEMP "lakehouse-streaming-jars"
New-Item -ItemType Directory -Force -Path $jarDir | Out-Null

Write-Host ""
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  ENTERPRISE DATA LAKEHOUSE - STREAMING DEMO" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  Started: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

Step "1/6" "Live streaming producer is publishing manufacturing events"
docker logs lakehouse_synthetic_datagen --tail 12

Step "2/6" "Bronze streaming table counts before Kafka append"
Trino "SELECT 'mes_production_orders' tbl, count(*) rows FROM iceberg.bronze.mes_production_orders UNION ALL SELECT 'iqms_quality_tests', count(*) FROM iceberg.bronze.iqms_quality_tests UNION ALL SELECT 'iqms_deviations', count(*) FROM iceberg.bronze.iqms_deviations UNION ALL SELECT 'trackwise_capas', count(*) FROM iceberg.bronze.trackwise_capas UNION ALL SELECT 'sap_ecc_orders', count(*) FROM iceberg.bronze.sap_ecc_orders UNION ALL SELECT 'tms_training_completions', count(*) FROM iceberg.bronze.tms_training_completions ORDER BY 1"

Step "3/6" "Prepare Spark Kafka and Iceberg runtime jars"
foreach ($jar in $jars) {
    docker cp "lakehouse_spark_master:/opt/bitnami/spark/.ivy2/jars/$jar" (Join-Path $jarDir $jar)
    docker cp (Join-Path $jarDir $jar) "lakehouse_airflow_scheduler:/tmp/$jar"
}

$jarArg = ($jars | ForEach-Object { "/tmp/$_" }) -join ","

Step "4/6" "Append new Kafka records into Bronze Iceberg"
docker exec lakehouse_airflow_scheduler bash -lc "spark-submit --jars $jarArg /opt/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py --topic all"

Step "5/6" "Refresh Silver and Gold models from stream-fed Bronze tables"
docker exec lakehouse_airflow_scheduler bash -lc "cd /opt/airflow/dbt && dbt run --select silver gold --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"
docker exec lakehouse_airflow_scheduler bash -lc "cd /opt/airflow/dbt && dbt test --select silver gold --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"

Step "6/6" "Streaming data is available in Bronze, Silver, Gold"
Write-Host ""
Write-Host "[BRONZE]"
Trino "SELECT 'mes_production_orders' tbl, count(*) rows FROM iceberg.bronze.mes_production_orders UNION ALL SELECT 'iqms_quality_tests', count(*) FROM iceberg.bronze.iqms_quality_tests UNION ALL SELECT 'iqms_deviations', count(*) FROM iceberg.bronze.iqms_deviations UNION ALL SELECT 'trackwise_capas', count(*) FROM iceberg.bronze.trackwise_capas UNION ALL SELECT 'sap_ecc_orders', count(*) FROM iceberg.bronze.sap_ecc_orders UNION ALL SELECT 'tms_training_completions', count(*) FROM iceberg.bronze.tms_training_completions ORDER BY 1"

Write-Host ""
Write-Host "[RECENT STREAMING SAMPLE]"
Trino "SELECT _source_system, _kafka_key, substr(_raw_payload, 1, 120) payload_preview, CAST(_ingested_at AS varchar) ingested_at FROM iceberg.bronze.mes_production_orders ORDER BY _ingested_at DESC LIMIT 3"

Write-Host ""
Write-Host "[SILVER]"
Trino "SELECT 'silver_mes_production_orders' tbl, count(*) rows FROM iceberg.silver.silver_mes_production_orders UNION ALL SELECT 'silver_iqms_quality_tests', count(*) FROM iceberg.silver.silver_iqms_quality_tests UNION ALL SELECT 'silver_iqms_deviations', count(*) FROM iceberg.silver.silver_iqms_deviations UNION ALL SELECT 'silver_sap_inventory', count(*) FROM iceberg.silver.silver_sap_inventory UNION ALL SELECT 'silver_tms_training', count(*) FROM iceberg.silver.silver_tms_training UNION ALL SELECT 'silver_trackwise_capas', count(*) FROM iceberg.silver.silver_trackwise_capas ORDER BY 1"

Write-Host ""
Write-Host "[GOLD]"
Trino "SELECT 'gold_manufacturing_oee_mart' tbl, count(*) rows FROM iceberg.gold.gold_manufacturing_oee_mart UNION ALL SELECT 'gold_quality_kpis', count(*) FROM iceberg.gold.gold_quality_kpis UNION ALL SELECT 'gold_sap_inventory_mart', count(*) FROM iceberg.gold.gold_sap_inventory_mart UNION ALL SELECT 'gold_training_compliance_mart', count(*) FROM iceberg.gold.gold_training_compliance_mart UNION ALL SELECT 'gold_compliance_capa_mart', count(*) FROM iceberg.gold.gold_compliance_capa_mart ORDER BY 1"

Write-Host ""
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  STREAMING PIPELINE COMPLETE" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  Kafka UI     : http://localhost:9000"
Write-Host "  Trino UI     : http://localhost:8180"
Write-Host "  Superset     : http://localhost:8500"
Write-Host "  Completed at : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
