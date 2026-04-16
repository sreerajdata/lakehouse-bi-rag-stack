# TPL Data Lakehouse — Claude Opus Master Build Prompt
## Use this in Cursor / Windsurf / Claude Code / any agentic AI coding environment

---

## ⚠️ HOW TO USE THIS PROMPT

1. Open your AI coding environment (Cursor, Windsurf, Claude Code, etc.)
2. Set **Claude Opus** (claude-opus-4-5 or latest) as your model
3. Open the `tpl-lakehouse/` project folder
4. Paste the **SYSTEM CONTEXT** block below as your system prompt or project instructions
5. Then use the **TASK PROMPTS** one by one in sequence
6. Each task prompt builds on the previous — complete them in order

---

## SYSTEM CONTEXT (paste as project instructions / system prompt)

```
You are a senior data engineer building a fully open-source, airgapped, on-premises
Data Lakehouse for TPL (Torrent Pharmaceuticals Ltd), a pharmaceutical manufacturer.

## Project Overview
- Storage: SeaweedFS (S3-compatible, replaces Dell ObjectScale)
- Table Format: Apache Iceberg (ACID, time-travel, schema evolution)
- Orchestration: Apache Airflow 2.8
- Processing: Apache Spark 3.5
- Transformation: dbt Core (Trino dialect)
- Query Engine: Apache Trino 435
- Streaming: Apache Kafka + Kafka Connect + Debezium CDC
- File Ingestion: Apache NiFi
- AI/LLM: Ollama (Llama 3, on-prem) + LangChain + Milvus
- Monitoring: Prometheus + Grafana + Loki
- Governance: DataHub + Apache Ranger + OpenSearch + OpenBao
- BI: Apache Superset
- Notebooks: JupyterHub
- CI/CD: GitLab CE
- Containers: Docker Compose (profiles-based, airgap-ready)

## Data Sources (synthetic data in dev)
- MES (Manufacturing Execution System) — MSSQL 4.7TB
- IQMS (Quality Management) — MSSQL 2.2TB
- Historian/L2 (OPC-UA time-series) — internal DB 2.4TB
- Trackwise (CAPA/QMS) — MSSQL 1.3TB
- SAP ECC (Financials/Inventory) — Oracle 9TB
- TMS (Training Management) — MSSQL 257GB

## Architecture: Medallion
- Bronze: Raw Kafka → Iceberg (ALCOA+ metadata tagged)
- Silver: dbt cleaning + Great Expectations DQ gates
- Gold: dbt domain marts (OEE, Compliance/CAPA, Inventory, Training)

## Regulatory Requirements
- 21 CFR Part 11 (FDA electronic records)
- EU Annex 11
- ALCOA+ data integrity principles
- Every record must carry: _source_system, _ingested_at, _row_hash, _kafka_offset

## Key Constraints
- FULLY AIRGAPPED in production (no internet access)
- ALL tooling must be open-source (no paid/cloud services)
- Docker Compose with profiles (core/ingestion/processing/lakehouse/analytics/ai/monitoring/governance/cicd)
- SeaweedFS acts as both the Iceberg warehouse and Milvus object store

## Project Structure
tpl-lakehouse/
├── docker-compose.yml         # Full stack with profiles
├── .env                       # All config/credentials
├── Makefile                   # Build commands
├── configs/
│   ├── airflow/dags/          # Airflow DAGs + Spark jobs
│   ├── trino/catalog/         # Iceberg + other catalogs
│   ├── hive/                  # Hive metastore site config
│   ├── spark/                 # Spark defaults
│   ├── prometheus/            # Scrape configs
│   ├── grafana/               # Dashboards + datasources
│   ├── loki/ promtail/        # Log aggregation
│   └── seaweedfs/             # S3 access config
├── dbt/
│   ├── models/bronze/         # Source declarations
│   ├── models/silver/         # Cleaning + enrichment
│   ├── models/gold/           # Domain data marts
│   └── tests/                 # dbt tests
├── synthetic_data/            # Fake data generators
├── dockerfiles/
│   └── langchain-app/         # RAG + Streamlit chatbot
└── scripts/                   # Init scripts

Always write production-quality, well-commented code.
Always add ALCOA+ metadata fields to any data pipeline you write.
Always use Iceberg's partitioning and time-travel features.
Always handle errors with proper retries and dead-letter logic.
Reference the existing files in the project before creating new ones.
```

---

## TASK PROMPTS (execute in sequence)

---

### TASK 1 — Validate & Complete Docker Compose

```
Read the existing docker-compose.yml carefully.

1. Identify any missing health checks on services that other services depend on
2. Add a `node-exporter` service to the monitoring profile for host metrics
3. Add a `postgres-exporter` service pointing to the PostgreSQL container
4. Verify all volume mounts reference paths that actually exist in the project
5. Add a `networks` alias for each service so internal DNS resolves correctly
6. Add the `restart: unless-stopped` policy to all stateful services
7. Output the corrected/updated docker-compose.yml sections only (don't rewrite the whole file)
```

---

### TASK 2 — Complete Silver Layer dbt Models

```
In dbt/models/silver/, create the following models that are currently missing.
Reference silver_mes_production_orders.sql as the pattern to follow.

Create:
1. silver_iqms_quality_tests.sql
   - Parse JSON from bronze.iqms_quality_tests
   - Add derived field: pass_fail_flag (boolean)
   - Add derived field: deviation_from_mean (result_value - 100) / 100
   - Filter: remove records where test_id IS NULL

2. silver_iqms_deviations.sql
   - Parse JSON from bronze.iqms_deviations
   - Categorize severity into numeric score (CRITICAL=3, MAJOR=2, MINOR=1)
   - Add days_to_close derived field

3. silver_trackwise_capas.sql
   - Parse JSON from bronze.trackwise_capas
   - Add is_overdue boolean field
   - Add days_open derived field
   - Add sla_breach_flag (TRUE if days_open > 90)

4. silver_sap_inventory.sql
   - Parse JSON from bronze.sap_inventory_movements
   - Classify movement_type into 'receipt', 'issue', 'transfer', 'scrap'
   - Compute running_balance_flag based on movement direction

5. silver_tms_training.sql
   - Parse JSON from bronze.tms_training_completions
   - Add training_overdue_flag
   - Add days_since_completion
   - Add certification_expiry_date (completion_date + validity_months * 30 days)

For each model, add dbt YAML schema.yml with:
- Column descriptions
- Not null tests on key columns
- Accepted values tests on status/result fields
- Unique tests on ID columns
```

---

### TASK 3 — Complete Gold Layer dbt Models

```
In dbt/models/gold/, the following marts are needed. 
Reference gold_manufacturing_oee_mart.sql as the pattern.

Create:

1. gold_sap_inventory_mart.sql
   Grain: material_code + plant + month
   Metrics: opening_stock, total_receipts, total_issues, closing_stock,
             stock_turnover_ratio, days_of_inventory, scrap_value_inr

2. gold_quality_risk_mart.sql
   Grain: product_code + batch_number
   Joins: silver_mes + silver_iqms_quality_tests + silver_iqms_deviations
   Metrics: total_tests, pass_rate, open_deviations, critical_deviation_count,
            batch_release_status (PASS/FAIL/PENDING), overall_risk_score

3. gold_training_compliance_mart.sql
   Grain: department + month
   Metrics: total_employees_trained, overdue_trainings, completion_rate_pct,
             gmp_compliance_score, training_rag_status (GREEN/AMBER/RED)

4. gold_supply_chain_mart.sql
   Grain: vendor_code + material_code + month
   Joins: silver_sap_inventory + silver_trackwise_capas (vendor-related)
   Metrics: on_time_delivery_pct, total_po_value_inr, avg_lead_time_days,
             quality_rejection_rate_pct, vendor_score

For each mart:
- Add a sources.yml entry pointing to the relevant silver models
- Add a meta block with: owner, regulatory_impact, refresh_frequency
- Add documentation strings for every column
```

---

### TASK 4 — Great Expectations DQ Checkpoints

```
In configs/airflow/dags/ge_checkpoints/, create a Great Expectations setup:

1. great_expectations.yml — GE project config pointing to Trino as datasource

2. Create checkpoint configs for:
   - mes_silver_checkpoint: validate silver_mes_production_orders
     Rules: yield_pct between 0-120, status in allowed list, no nulls on order_id
   
   - iqms_silver_checkpoint: validate silver_iqms_quality_tests
     Rules: result_value between 80-120, result in [PASS, FAIL], test_type not null
   
   - sap_silver_checkpoint: validate silver_sap_inventory
     Rules: quantity > 0, valuation_amount_inr > 0, posting_date not null

3. run_checkpoint.py — Python script that:
   - Connects to Trino via SQLAlchemy
   - Runs the named checkpoint
   - Returns exit code 0 on pass, 1 on fail
   - Writes results to s3a://lakehouse-gold/dq_results/ as JSON

4. Create a GE DataDocs site config that publishes HTML reports to:
   s3a://lakehouse-docs/ge_reports/
```

---

### TASK 5 — Kafka Debezium Connectors

```
In configs/kafka-connect/connectors/, create Kafka Connect connector JSON configs
for CDC from the synthetic PostgreSQL source tables:

1. mes-postgres-connector.json
   - Debezium PostgreSQL connector
   - Source: postgres:5432, database: lakehouse_meta
   - Tables: public.mes_production_orders
   - Topic prefix: cdc.mes
   - Include BEFORE/AFTER state in events
   - Heartbeat interval: 60s

2. iqms-postgres-connector.json
   - Same pattern for iqms_quality_tests table

3. tms-postgres-connector.json
   - Same pattern for tms table

Also create a connector registration script:
scripts/register_connectors.sh
  - Uses curl to POST each connector config to http://kafka-connect:8083/connectors
  - Checks if connector already exists before creating
  - Outputs status for each connector
  - Can be called from Makefile as: make register-connectors
```

---

### TASK 6 — LangChain Document Indexer

```
In dockerfiles/langchain-app/utils/, create:

1. index_documents.py
   A script that:
   - Reads PDFs/docs from a SeaweedFS bucket (via boto3 with S3 endpoint override)
   - Extracts text using PyMuPDF (fitz) for PDFs
   - Falls back to Apache Tika REST API (http://tika:9998) for non-PDF files
   - Chunks text into 512-token chunks with 50-token overlap
   - Generates embeddings using OllamaEmbeddings (model: llama3)
   - Upserts vectors into Milvus collection 'tpl_documents'
   - Stores metadata: filename, page_number, source_system, ingested_at
   - Accepts CLI args: --bucket, --collection, --tika-url

2. trino_tool.py
   A LangChain custom tool that:
   - Connects to Trino at trino:8080
   - Has a run() method accepting a SQL question as natural language
   - Uses LLM to convert the question to SQL against the gold layer tables
   - Executes the SQL and returns results as a formatted string
   - Includes safety: only allows SELECT statements, no DDL/DML
   - Returns "No data found" gracefully if result is empty

3. document_qa.py
   A standalone QA chain that:
   - Takes a question and an optional document source filter
   - Retrieves top-k chunks from Milvus
   - Uses Llama 3 to synthesize an answer with citations
   - Returns: {answer: str, sources: list[str], confidence: float}
```

---

### TASK 7 — Grafana Dashboards as Code

```
In configs/grafana/provisioning/dashboards/, create JSON dashboard definitions for:

1. manufacturing_oee.json
   Panels:
   - OEE Score gauge (target: 0.85)
   - Availability / Performance / Quality trend lines (7-day)
   - Production volume bar chart by machine
   - Top 5 machines by scrap percentage (table)
   - Real-time machine status heatmap
   Data source: Prometheus (for live) + PostgreSQL (for historical)

2. data_pipeline_health.json
   Panels:
   - Kafka consumer lag per topic (line chart)
   - Airflow DAG success rate (stat panel)
   - Spark job duration histogram
   - SeaweedFS storage utilization (gauge)
   - Bronze/Silver/Gold row counts (stats)
   - Failed Airflow tasks in last 24h (table with links)

3. compliance_audit.json
   Panels:
   - Open CAPAs by department (bar chart)
   - CAPA RAG status distribution (pie)
   - Quality test pass rate trend (line)
   - Overdue training count (stat with alert threshold)
   - Recent audit log entries (log panel from Loki)

Each dashboard JSON must:
- Use Grafana 10.x schema
- Reference datasource by name (not hardcoded UID)
- Include templating variables: $plant, $shift, $date_range
- Have a consistent dark-mode color theme
```

---

### TASK 8 — DataHub Metadata Ingestion

```
In configs/datahub/, create:

1. trino_recipe.yml
   DataHub ingestion recipe for Trino/Iceberg:
   - Source: trino connector pointing to trino:8080
   - Catalogs to scan: iceberg (all schemas: bronze, silver, gold)
   - Include: table descriptions, column lineage, statistics
   - Emit to: datahub-gms:8080

2. airflow_recipe.yml
   DataHub Airflow plugin config:
   - Capture DAG runs and task lineage
   - Map Airflow task → Iceberg table lineage
   - Emit pipeline runs to DataHub

3. lineage_builder.py
   Python script that:
   - Reads dbt manifest.json (generated by dbt docs generate)
   - Extracts model → model dependencies
   - Emits DataHub DatasetLineageClass events via REST API
   - Maps: bronze table → silver model → gold mart as full lineage chain

4. datahub_glossary.yml
   Business glossary with terms:
   - OEE, Availability, Performance, Quality Rate
   - CAPA, Deviation, Batch Record
   - ALCOA+, 21 CFR Part 11
   Each term: name, definition, domain (Manufacturing/Compliance/Quality)
```

---

### TASK 9 — Airflow DAG: Document PDF Processing Pipeline

```
In configs/airflow/dags/, create pdf_processing_pipeline.py:

A DAG that runs hourly and:
1. Lists new PDF files in s3a://lakehouse-docs/ (files added since last run)
2. For each PDF:
   a. Downloads it from SeaweedFS via boto3
   b. Sends to Apache Tika (http://tika:9998/tika) for text extraction
   c. For scanned PDFs (where Tika returns minimal text):
      - Converts PDF pages to images using pdf2image
      - Sends images to Tesseract OCR (http://tesseract:3000)
   d. Stores extracted text to s3a://lakehouse-bronze/docs/{filename}.txt
   e. Adds metadata record to Iceberg table: lakehouse.bronze.document_registry
      Columns: doc_id, filename, source_bucket, extracted_text_path,
               page_count, extraction_method, extracted_at, _row_hash

3. Triggers the LangChain indexer to add new docs to Milvus
4. Sends success/failure notification to Grafana Loki

Include:
- Proper XCom for passing file lists between tasks
- Sensor task that waits for at least 1 new file before proceeding
- Error handling: failed files go to s3a://lakehouse-bronze/docs/failed/
- Max concurrency: 3 PDFs processed in parallel
```

---

### TASK 10 — Full Stack Integration Test

```
Create scripts/integration_test.py that validates the entire pipeline end-to-end:

Test Suite:
1. test_seaweedfs_connectivity()
   - Creates a test object, reads it back, deletes it
   - Asserts all 6 buckets exist

2. test_kafka_flow()
   - Publishes 10 test messages to mes.production_orders
   - Consumes them back within 10 seconds
   - Asserts message content integrity

3. test_bronze_iceberg()
   - Queries Trino: SELECT COUNT(*) FROM iceberg.bronze.mes_production_orders
   - Asserts count > 0
   - Asserts _row_hash column is not null
   - Asserts _ingested_at is within last 24h

4. test_silver_quality()
   - Queries silver_mes_production_orders
   - Asserts yield_pct between 0 and 120 for all records
   - Asserts no null order_ids

5. test_gold_oee()
   - Queries gold_manufacturing_oee_mart
   - Asserts oee_score between 0 and 1
   - Asserts at least 5 distinct machines

6. test_trino_query_performance()
   - Runs a complex aggregation on gold layer
   - Asserts query completes in < 30 seconds

7. test_ollama_health()
   - Pings http://ollama:11434/api/tags
   - Asserts llama3 model is available

8. test_milvus_connectivity()
   - Connects to Milvus
   - Asserts collection 'tpl_documents' exists

9. test_airflow_dag_success()
   - Checks last DAG run for tpl_medallion_pipeline
   - Asserts state == 'success'

10. test_grafana_datasources()
    - Checks Grafana API for datasource health
    - Asserts Prometheus and Loki are both 'OK'

Output: JUnit XML report to scripts/test_results.xml
Add to Makefile as: make integration-test
```

---

## BONUS PROMPTS (for advanced features)

---

### BONUS A — RBAC with Apache Ranger

```
Create configs/ranger/ with:
1. Ranger admin setup for Trino policies
2. Policy definitions:
   - data_engineer: full access to bronze, silver, gold
   - analyst: read-only on silver and gold
   - qa_team: read-only on gold compliance mart
   - executive: read-only on gold OEE and training mart
3. Row-level security policy for plant-based data isolation
4. Column masking policy: mask batch_number for non-QA roles
5. Audit policy: log all SELECT on gold layer to OpenSearch
```

---

### BONUS B — Iceberg Table Maintenance DAG

```
Create configs/airflow/dags/iceberg_maintenance.py:
A daily DAG that runs Spark jobs to:
1. EXPIRE SNAPSHOTS older than 7 days on all bronze tables
2. REWRITE DATA FILES (compact small files) on silver tables
3. REWRITE MANIFESTS on gold tables
4. Run ANALYZE TABLE on all gold tables to update statistics for Trino
5. Archive expired snapshots manifest to s3a://lakehouse-gold/archive/
6. Emit maintenance metrics to Prometheus pushgateway
```

---

### BONUS C — Streamlit Admin Dashboard

```
Create dockerfiles/admin-dashboard/app.py:
A Streamlit app that shows:
1. Live pipeline status (Airflow DAG states via API)
2. Storage utilization per bucket (SeaweedFS metrics)
3. Kafka topic lag chart (Kafka Admin API)
4. Data freshness table (last _ingested_at per bronze table)
5. Failed records count per source (from DQ checkpoint results)
6. One-click buttons: Trigger DAG, Rerun Failed Tasks, Compact Tables
All backed by direct API calls — no database required
```

---

## DEBUGGING PROMPTS (use when things break)

```
# If Hive Metastore fails to start:
"The Hive Metastore container is failing. Read configs/hive/hive-site.xml
and docker-compose.yml. The error is: [paste error]. 
Diagnose the root cause and provide the exact fix."

# If Trino can't read Iceberg tables:
"Trino returns 'Table does not exist' for iceberg.bronze.mes_production_orders.
The table was created by the Spark job. Read configs/trino/catalog/iceberg.properties
and the Spark job at configs/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py.
Identify the mismatch and fix it."

# If dbt fails to connect to Trino:
"dbt run fails with connection error. Read dbt/profiles.yml and 
docker-compose.yml service definitions for dbt and trino.
Diagnose the network/port issue and provide the fix."

# If Milvus can't connect to SeaweedFS:
"Milvus fails to store vectors with S3 error. Read docker-compose.yml
Milvus environment section and configs/seaweedfs/s3.json.
The error is: [paste error]. Fix the SeaweedFS S3 compatibility issue."
```
