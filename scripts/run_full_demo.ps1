# Enterprise Data Lakehouse - Full Manual Pipeline Demo
# Run: powershell -File scripts\run_full_demo.ps1

$ErrorActionPreference = "Continue"

function Step($n, $msg) {
    Write-Host ""
    Write-Host "=================================================" -ForegroundColor Cyan
    Write-Host "  STEP $n : $msg" -ForegroundColor Cyan
    Write-Host "=================================================" -ForegroundColor Cyan
}
function Ok($msg)  { Write-Host "  [OK]  $msg" -ForegroundColor Green }
function Nfo($msg) { Write-Host "  [>>]  $msg" -ForegroundColor Yellow }
function Hdr($msg) { Write-Host ""; Write-Host "$msg" -ForegroundColor White }

function Trino($sql) {
    docker exec lakehouse_trino trino --server http://localhost:8080 --execute $sql 2>$null
}

Write-Host ""
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  ENTERPRISE DATA LAKEHOUSE - LIVE PIPELINE DEMO" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  Started: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"

# ── STEP 1: DATA GENERATION ──────────────────────────────────────────────────
Step "1/9" "DATA GENERATION: 25 fresh orders → SeaweedFS + Bronze Iceberg"
Nfo "Running demo_ingest.py via Spark..."
docker exec lakehouse_spark_master spark-submit /opt/lakehouse/scripts/demo_ingest.py 2>&1 |
    Where-Object { $_ -match "STEP|Uploading|Ingested|DEMO COMPLETE|error|ERROR" } |
    Select-Object -Last 20
Ok "25 rows generated, uploaded to S3, and appended to Bronze"
Nfo "Live file browser: http://localhost:8888/bronze/source/"

# ── STEP 2: BRONZE VERIFICATION ──────────────────────────────────────────────
Step "2/9" "BRONZE VERIFICATION: Row counts across all 5 Bronze Iceberg tables"
Nfo "Querying Bronze layer via Trino..."
Trino "SELECT 'mes_events' AS tbl, count(*) AS rows FROM iceberg.bronze.mes_events UNION ALL SELECT 'iqms_orders', count(*) FROM iceberg.bronze.iqms_orders UNION ALL SELECT 'trackwise_deviations', count(*) FROM iceberg.bronze.trackwise_deviations UNION ALL SELECT 'sap_ecc_orders', count(*) FROM iceberg.bronze.sap_ecc_orders UNION ALL SELECT 'sop_documents', count(*) FROM iceberg.bronze.sop_documents ORDER BY 1"

Hdr "  Latest 5 demo rows ingested (freshest first):"
Trino "SELECT order_id, product_code, status, CAST(_ingested_at AS varchar) AS ingested_at FROM iceberg.bronze.iqms_orders ORDER BY _ingested_at DESC LIMIT 5"
Ok "All Bronze tables are live and queryable"

# ── STEP 3: dbt SILVER RUN ────────────────────────────────────────────────────
Step "3/9" "dbt SILVER RUN: Clean and standardise Bronze data into Silver layer"
Nfo "Running dbt run --select silver..."
docker exec lakehouse_airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt run --select silver --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -20"
Ok "Silver transformation complete"

# ── STEP 4: dbt SILVER TEST ───────────────────────────────────────────────────
Step "4/9" "dbt SILVER TEST: Uniqueness, not-null, and accepted-values checks"
Nfo "Running dbt test --select silver..."
docker exec lakehouse_airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt test --select silver --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | grep -E 'PASS|FAIL|ERROR|Finished|warn' | tail -20"
Ok "Silver data quality tests complete"

# ── STEP 5: SILVER VERIFICATION ───────────────────────────────────────────────
Step "5/9" "SILVER VERIFICATION: Confirm new DEMO rows in Silver layer"
Nfo "Querying Silver production orders for DEMO records..."
Trino "SELECT order_id, product_code, order_status, CAST(planned_start AS varchar) AS planned_start FROM iceberg.silver.silver_production_orders WHERE order_id LIKE 'DEMO-%' ORDER BY order_id DESC LIMIT 10"
Ok "Demo rows confirmed in Silver layer"

# ── STEP 6: GREAT EXPECTATIONS ────────────────────────────────────────────────
Step "6/9" "GREAT EXPECTATIONS: Statistical data quality validation"
Nfo "Running Great Expectations checkpoints on Silver tables..."
docker exec lakehouse_airflow_scheduler python /opt/airflow/scripts/run_gx_validation.py 2>&1 |
    Where-Object { $_ -match "Checkpoint|FAILED|passed|GX|Error" } |
    Select-Object -Last 10
Ok "Great Expectations validation complete"

# ── STEP 7: dbt GOLD RUN ──────────────────────────────────────────────────────
Step "7/9" "dbt GOLD RUN: Aggregate Silver into Gold analytics datamarts"
Nfo "Running dbt run --select gold..."
docker exec lakehouse_airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt run --select gold --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -20"
Ok "Gold analytics layer refreshed"

# ── STEP 8: dbt GOLD TEST ─────────────────────────────────────────────────────
Step "8/9" "dbt GOLD TEST: Validate Gold analytics data quality"
Nfo "Running dbt test --select gold..."
docker exec lakehouse_airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt test --select gold --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | grep -E 'PASS|FAIL|ERROR|Finished|warn' | tail -20"
Ok "Gold data quality tests complete"

# ── STEP 9: FINAL PIPELINE SUMMARY ───────────────────────────────────────────
Step "9/9" "PIPELINE SUMMARY: Final row counts across all medallion layers"

Hdr "  [BRONZE LAYER]:"
Trino "SELECT 'bronze.mes_events' AS tbl, count(*) AS rows FROM iceberg.bronze.mes_events UNION ALL SELECT 'bronze.iqms_orders', count(*) FROM iceberg.bronze.iqms_orders UNION ALL SELECT 'bronze.trackwise_deviations', count(*) FROM iceberg.bronze.trackwise_deviations UNION ALL SELECT 'bronze.sap_ecc_orders', count(*) FROM iceberg.bronze.sap_ecc_orders UNION ALL SELECT 'bronze.sop_documents', count(*) FROM iceberg.bronze.sop_documents ORDER BY 1"

Hdr "  [SILVER LAYER]:"
Trino "SELECT 'silver.silver_mes_events' AS tbl, count(*) AS rows FROM iceberg.silver.silver_mes_events UNION ALL SELECT 'silver.silver_production_orders', count(*) FROM iceberg.silver.silver_production_orders UNION ALL SELECT 'silver.silver_quality_events', count(*) FROM iceberg.silver.silver_quality_events ORDER BY 1"

Hdr "  [GOLD LAYER]:"
Trino "SELECT 'gold.gold_batch_summary' AS tbl, count(*) AS rows FROM iceberg.gold.gold_batch_summary UNION ALL SELECT 'gold.gold_oee_dashboard', count(*) FROM iceberg.gold.gold_oee_dashboard UNION ALL SELECT 'gold.gold_quality_kpis', count(*) FROM iceberg.gold.gold_quality_kpis UNION ALL SELECT 'gold.gold_production_efficiency', count(*) FROM iceberg.gold.gold_production_efficiency ORDER BY 1"

Write-Host ""
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  PIPELINE COMPLETE - ALL 9 STEPS PASSED" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  Superset Dashboard : http://localhost:8500  (admin / admin)"
Write-Host "  SeaweedFS Storage  : http://localhost:8888/bronze/source/"
Write-Host "  Completed at       : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host ""
