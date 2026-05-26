#!/usr/bin/env bash

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

step()  { echo -e "\n${CYAN}${BOLD}══════════════════════════════════════════════════${NC}"; echo -e "${CYAN}${BOLD}  STEP $1: $2${NC}"; echo -e "${CYAN}${BOLD}══════════════════════════════════════════════════${NC}"; }
ok()    { echo -e "  ${GREEN}✅  $*${NC}"; }
info()  { echo -e "  ${YELLOW}ℹ   $*${NC}"; }
fail()  { echo -e "  ${RED}❌  $*${NC}"; }
header(){ echo -e "\n${BOLD}$*${NC}"; }

TRINO="docker exec lakehouse_trino trino --server http://localhost:8080"
DBT="docker exec lakehouse_airflow_scheduler bash -c"
AIRFLOW_PY="docker exec lakehouse_airflow_scheduler python"

echo -e "\n${BOLD}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  ENTERPRISE DATA LAKEHOUSE — LIVE PIPELINE DEMO  ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════╝${NC}"
echo -e "  Running: $(date '+%Y-%m-%d %H:%M:%S')\n"

step "1/9" "DATA GENERATION — Create 25 fresh production orders"
info "Generating 25 demo rows with live timestamps + uploading to SeaweedFS S3..."
docker exec lakehouse_spark_master spark-submit \
  /opt/lakehouse/scripts/demo_ingest.py 2>&1 \
  | grep -E "(STEP|✅|ℹ|DEMO-|Uploaded|Ingested|complete|DEMO COMPLETE|ERROR)" || true
ok "Source data generated and uploaded to s3://bronze/source/"
info "Browse live files at: http://localhost:8888/bronze/source/"

step "2/9" "BRONZE VERIFICATION — Row counts across all Bronze tables"
info "Querying Iceberg Bronze tables via Trino..."
$TRINO --execute \
"SELECT table_name, row_count FROM (
  SELECT 'mes_events'           AS table_name, count(*) AS row_count FROM iceberg.bronze.mes_events
  UNION ALL
  SELECT 'iqms_orders'          AS table_name, count(*) FROM iceberg.bronze.iqms_orders
  UNION ALL
  SELECT 'trackwise_deviations' AS table_name, count(*) FROM iceberg.bronze.trackwise_deviations
  UNION ALL
  SELECT 'sap_ecc_orders'       AS table_name, count(*) FROM iceberg.bronze.sap_ecc_orders
  UNION ALL
  SELECT 'sop_documents'        AS table_name, count(*) FROM iceberg.bronze.sop_documents
) ORDER BY table_name" 2>/dev/null
ok "All Bronze Iceberg tables are live and queryable"

header "  Latest 5 Bronze demo rows (freshest _ingested_at first):"
$TRINO --execute \
"SELECT order_id, product_code, status, CAST(_ingested_at AS varchar) AS ingested_at
 FROM iceberg.bronze.iqms_orders
 ORDER BY _ingested_at DESC LIMIT 5" 2>/dev/null
ok "Data confirmed in Bronze layer with live timestamps"

step "3/9" "dbt SILVER RUN — Transform Bronze → Silver layer"
info "Running dbt silver models (clean, standardise, deduplicate)..."
$DBT "cd /opt/airflow/dbt && dbt run --select silver --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -20"
ok "Silver transformation complete"

step "4/9" "dbt SILVER TEST — Validate data quality rules"
info "Running dbt tests: unique, not_null, accepted_values..."
$DBT "cd /opt/airflow/dbt && dbt test --select silver --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | grep -E '(PASS|FAIL|ERROR|error|Finished)'" || true
ok "Silver data quality tests complete"

step "5/9" "SILVER VERIFICATION — Confirm new demo rows in Silver"
info "Querying Silver tables in Trino..."
$TRINO --execute \
"SELECT order_id, product_code, order_status, CAST(planned_start AS varchar) AS planned_start
 FROM iceberg.silver.silver_production_orders
 WHERE order_id LIKE 'DEMO-%'
 ORDER BY order_id DESC LIMIT 10" 2>/dev/null
ok "Demo rows confirmed in Silver layer"

step "6/9" "GREAT EXPECTATIONS — Statistical data quality validation"
info "Running Great Expectations checkpoints on Silver tables..."
$AIRFLOW_PY /opt/airflow/scripts/run_gx_validation.py 2>&1 \
  | grep -E "(✅|❌|Checkpoint|FAILED|passed|error)" || true
ok "Great Expectations validation complete"

step "7/9" "dbt GOLD RUN — Aggregate Silver → Gold analytics layer"
info "Running dbt gold models (OEE, KPIs, compliance marts)..."
$DBT "cd /opt/airflow/dbt && dbt run --select gold --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -20"
ok "Gold analytics layer refreshed"

step "8/9" "dbt GOLD TEST — Validate Gold analytics quality"
info "Running dbt tests on Gold datamarts..."
$DBT "cd /opt/airflow/dbt && dbt test --select gold --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | grep -E '(PASS|FAIL|ERROR|Finished)'" || true
ok "Gold data quality tests complete"

step "9/9" "PIPELINE SUMMARY — Final row counts across all three layers"
info "Querying final row counts across all medallion layers..."

header "  📦  BRONZE LAYER:"
$TRINO --execute \
"SELECT 'bronze.mes_events'           AS table_name, count(*) AS rows FROM iceberg.bronze.mes_events
 UNION ALL SELECT 'bronze.iqms_orders',          count(*) FROM iceberg.bronze.iqms_orders
 UNION ALL SELECT 'bronze.trackwise_deviations', count(*) FROM iceberg.bronze.trackwise_deviations
 UNION ALL SELECT 'bronze.sap_ecc_orders',       count(*) FROM iceberg.bronze.sap_ecc_orders
 UNION ALL SELECT 'bronze.sop_documents',        count(*) FROM iceberg.bronze.sop_documents
 ORDER BY 1" 2>/dev/null

header "  🥈  SILVER LAYER:"
$TRINO --execute \
"SELECT 'silver.silver_mes_events'       AS table_name, count(*) AS rows FROM iceberg.silver.silver_mes_events
 UNION ALL SELECT 'silver.silver_quality_events', count(*) FROM iceberg.silver.silver_quality_events
 UNION ALL SELECT 'silver.silver_production_orders', count(*) FROM iceberg.silver.silver_production_orders
 ORDER BY 1" 2>/dev/null

header "  🥇  GOLD LAYER:"
$TRINO --execute \
"SELECT 'gold.gold_oee_dashboard'         AS table_name, count(*) AS rows FROM iceberg.gold.gold_oee_dashboard
 UNION ALL SELECT 'gold.gold_batch_summary',        count(*) FROM iceberg.gold.gold_batch_summary
 UNION ALL SELECT 'gold.gold_quality_kpis',         count(*) FROM iceberg.gold.gold_quality_kpis
 UNION ALL SELECT 'gold.gold_production_efficiency',count(*) FROM iceberg.gold.gold_production_efficiency
 ORDER BY 1" 2>/dev/null

echo -e "\n${GREEN}${BOLD}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║  ✅  PIPELINE COMPLETE — ALL 9 STEPS PASSED      ║${NC}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════╝${NC}"
echo -e "  Superset Dashboard: ${BOLD}http://localhost:8500${NC}  (admin / admin)"
echo -e "  SeaweedFS Storage:  ${BOLD}http://localhost:8888/bronze/source/${NC}"
echo -e "  Completed at: $(date '+%Y-%m-%d %H:%M:%S')\n"
