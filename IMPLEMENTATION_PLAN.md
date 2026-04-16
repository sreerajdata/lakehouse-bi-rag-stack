# TPL Data Lakehouse — Full Implementation Plan
## Airgapped On-Prem Docker Stack | SeaweedFS + Apache Iceberg + OSS Tooling

---

## Architecture Summary

```
DATA SOURCES  →  INGESTION  →  BRONZE  →  SILVER  →  GOLD  →  SERVING
(Synthetic)      Kafka/NiFi     Iceberg    dbt/GE     dbt      Trino/Superset/AI
                 Debezium       SeaweedFS
```

**Storage Backend:** SeaweedFS S3 (replaces Dell ObjectScale)
**Table Format:** Apache Iceberg (ACID, time-travel, schema evolution)
**Compute:** Apache Spark 3.5
**Orchestration:** Apache Airflow 2.8
**Transformation:** dbt Core (Trino dialect)
**Query Engine:** Apache Trino 435
**AI:** Ollama (Llama 3) + LangChain + Milvus

---

## Phase 0 — Environment Prerequisites

### System Requirements (Minimum for Dev)

| Resource | Minimum | Recommended |
|---|---|---|
| CPU Cores | 8 | 16+ |
| RAM | 32 GB | 64 GB |
| Disk | 200 GB | 500 GB SSD |
| OS | Ubuntu 22.04 / macOS 14 | Ubuntu 22.04 |
| Docker | 24.x+ | Latest |
| Docker Compose | 2.20+ | Latest |

### Install Prerequisites

```bash
# Ubuntu
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin make git curl

# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
docker compose version
make --version
```

### Project Setup

```bash
git clone <your-repo>/tpl-lakehouse.git
cd tpl-lakehouse

# Copy and configure env
cp .env.example .env
# Edit .env if needed (passwords, bucket names, etc.)

# Prepare scripts
chmod +x scripts/*.sh
```

---

## Phase 1 — Core Infrastructure (Day 1)

### 1.1 Start Core Layer

```bash
make up-core
```

**Services started:** PostgreSQL, SeaweedFS (master + volume + filer + S3 gateway)

### 1.2 Validate SeaweedFS

```bash
# Check SeaweedFS master
curl http://localhost:9333/cluster/status

# List S3 buckets (auto-created by init script)
aws --endpoint-url=http://localhost:8333 \
    --no-verify-ssl \
    s3 ls

# Expected buckets:
# lakehouse-bronze
# lakehouse-silver
# lakehouse-gold
# lakehouse-models
# lakehouse-docs
# milvus-bucket
```

### 1.3 Validate PostgreSQL

```bash
docker exec -it lakehouse_postgres psql -U admin -c "\l"
# Expected: airflow, hive_metastore, superset, lakehouse_meta databases
```

**✅ Phase 1 Complete Criteria:**
- SeaweedFS master/volume/filer/S3 all healthy
- All 5 S3 buckets exist
- PostgreSQL has 4 databases created

---

## Phase 2 — Data Ingestion Layer (Day 1–2)

### 2.1 Start Ingestion

```bash
make up-ingestion
```

**Services started:** Zookeeper, Kafka, Schema Registry, Kafka Connect (+ Debezium), NiFi, Kafka UI

### 2.2 Validate Kafka

```bash
# Check Kafka topics (after synthetic data starts)
make kafka-topics

# Kafka UI (browser)
open http://localhost:9000
```

### 2.3 Start Synthetic Data Generator

```bash
make up-synthetic
```

This creates streaming events into these Kafka topics:

| Topic | Source | Rate |
|---|---|---|
| `mes.production_orders` | MES | ~2/sec |
| `mes.machine_status` | MES | ~2/sec |
| `mes.oee_metrics` | MES | ~1/sec |
| `iqms.quality_tests` | IQMS | ~2/sec |
| `iqms.deviations` | IQMS | ~0.2/sec |
| `historian.process_parameters` | L2/Historian | ~10/sec |
| `trackwise.capas` | Trackwise | ~0.1/sec |
| `sap.inventory_movements` | SAP ECC | ~1/sec |
| `tms.training_completions` | TMS | ~0.3/sec |

### 2.4 Validate Message Flow

```bash
# Check message count per topic in Kafka UI
open http://localhost:9000

# Or via CLI
docker exec lakehouse_kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups
```

### 2.5 Configure NiFi (Optional — for File/OPC-UA ingestion)

```bash
open http://localhost:8090
# Login: admin / adminadminadmin
# Import flow template from: configs/nifi/templates/
```

**✅ Phase 2 Complete Criteria:**
- All 9 Kafka topics receiving messages
- Kafka Connect running with Debezium connectors installed
- NiFi accessible and ready

---

## Phase 3 — Processing Layer (Day 2–3)

### 3.1 Start Processing

```bash
make up-processing
```

**Services started:** Spark Master + 2 Workers, Airflow (init + webserver + scheduler + worker), Redis, dbt, Tika, Tesseract

### 3.2 Validate Spark

```bash
# Spark Master UI
open http://localhost:8181

# Test Spark shell with Iceberg + SeaweedFS
make spark-shell
# scala> spark.sql("SHOW NAMESPACES IN lakehouse").show()
```

### 3.3 Validate Airflow

```bash
open http://localhost:8280
# Login: admin / admin

# Trigger the medallion pipeline manually
# DAG: tpl_medallion_pipeline
```

### 3.4 First Full Pipeline Run

```bash
# Unpause and trigger the DAG
docker exec lakehouse_airflow_web \
  airflow dags unpause tpl_medallion_pipeline

docker exec lakehouse_airflow_web \
  airflow dags trigger tpl_medallion_pipeline
```

**Monitor in Airflow UI → DAGs → tpl_medallion_pipeline → Grid view**

**✅ Phase 3 Complete Criteria:**
- Spark master shows 2 workers connected
- Airflow shows all tasks passing (green) on first run
- Bronze Iceberg tables created in SeaweedFS

---

## Phase 4 — Lakehouse Layer (Day 3)

### 4.1 Start Lakehouse

```bash
make up-lakehouse
```

**Services started:** Hive Metastore (PostgreSQL backend), Trino 435

### 4.2 Validate Hive Metastore

```bash
docker logs lakehouse_hive_metastore | grep "Started"
# Expected: "Starting Metastore Server"
```

### 4.3 Validate Trino + Iceberg

```bash
make trino-shell

# In Trino CLI:
trino> SHOW CATALOGS;
trino> SHOW SCHEMAS IN iceberg;
trino> SHOW TABLES IN iceberg.bronze;

# Query bronze layer
trino> SELECT COUNT(*) FROM iceberg.bronze.mes_production_orders;

# Check time-travel
trino> SELECT * FROM iceberg.bronze.mes_production_orders
       FOR TIMESTAMP AS OF TIMESTAMP '2024-01-01 00:00:00';
```

### 4.4 Create Iceberg Namespaces

```bash
make trino-shell
# Create namespaces
trino> CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
trino> CREATE SCHEMA IF NOT EXISTS iceberg.silver;
trino> CREATE SCHEMA IF NOT EXISTS iceberg.gold;
```

**✅ Phase 4 Complete Criteria:**
- Trino shows `iceberg` catalog with bronze/silver/gold schemas
- Bronze tables queryable via Trino
- Table history and snapshots visible

---

## Phase 5 — dbt Silver & Gold Transformations (Day 3–4)

### 5.1 Run dbt Silver Models

```bash
make dbt-run
# or selectively:
docker exec lakehouse_dbt dbt run --select silver

# Test data quality
make dbt-test
```

### 5.2 Silver Table Verification

```bash
make trino-shell
trino> SELECT
         COUNT(*),
         AVG(yield_pct),
         COUNT(DISTINCT machine_id)
       FROM iceberg.silver.silver_mes_production_orders;
```

### 5.3 Run Gold Marts

```bash
docker exec lakehouse_dbt dbt run --select gold
```

### 5.4 Verify Gold Layer

```bash
trino> SELECT
         machine_id, shift, oee_score, quality_pass_rate_pct
       FROM iceberg.gold.gold_manufacturing_oee_mart
       ORDER BY production_date DESC
       LIMIT 20;

trino> SELECT
         department, report_month, closure_rate_pct, compliance_rag_status
       FROM iceberg.gold.gold_compliance_capa_mart
       WHERE compliance_rag_status = 'RED';
```

**✅ Phase 5 Complete Criteria:**
- All dbt models run without errors
- Silver tables have cleaned, enriched data
- Gold marts queryable with calculated KPIs
- Great Expectations DQ checks passing

---

## Phase 6 — Analytics & BI (Day 4–5)

### 6.1 Start Analytics Layer

```bash
make up-analytics
```

**Services started:** JupyterHub, Apache Superset

### 6.2 Configure Superset

```bash
open http://localhost:8500
# Login: admin / admin

# Add Trino connection:
# Settings → Database Connections → Add Database
# Database Type: Trino
# SQLAlchemy URI: trino://admin@trino:8080/iceberg
```

### 6.3 Create Dashboards in Superset

Key dashboards to build:
1. **Manufacturing OEE Dashboard** (source: `gold_manufacturing_oee_mart`)
2. **Quality & Compliance Dashboard** (source: `gold_compliance_capa_mart`)
3. **Inventory & Supply Chain** (source: `gold_sap_inventory_mart`)
4. **Training Compliance** (source: `gold_tms_compliance_mart`)

### 6.4 JupyterHub Notebooks

```bash
open http://localhost:8400
# Login: admin (create password on first login)

# Sample notebook to test Trino from Jupyter:
import trino
conn = trino.dbapi.connect(
    host="trino", port=8080, user="admin", catalog="iceberg"
)
df = pd.read_sql("SELECT * FROM gold.gold_manufacturing_oee_mart LIMIT 100", conn)
```

**✅ Phase 6 Complete Criteria:**
- Superset connected to Trino/Iceberg
- At least 2 dashboards built from Gold layer
- JupyterHub accessible with PySpark/Trino kernels

---

## Phase 7 — AI & Intelligent Insights (Day 5–6)

### 7.1 Start AI Layer

```bash
make up-ai
```

**Services started:** Ollama (Llama 3), Milvus (vector DB), LangChain + Streamlit chatbot

### 7.2 Pull Llama 3 Model

```bash
make ollama-pull-llama3
# This downloads ~4GB, takes 5-15 min depending on speed
# For airgap: pre-download and mount the model volume
```

### 7.3 Index Documents into Milvus

```bash
# Upload documents (SOPs, batch records) to SeaweedFS
aws --endpoint-url=http://localhost:8333 s3 cp \
    ./docs/sop_gmp_001.pdf s3://lakehouse-docs/

# Run the document indexer
docker exec lakehouse_langchain_app \
    python utils/index_documents.py \
    --bucket lakehouse-docs \
    --collection tpl_documents
```

### 7.4 Test AI Chatbot

```bash
open http://localhost:8501

# Test queries:
# "What is the OEE for machine MCH-001 this week?"
# "Show me all CAPAs overdue by more than 30 days"
# "What are the 21 CFR Part 11 requirements for audit trails?"
```

**✅ Phase 7 Complete Criteria:**
- Llama 3 responding via Ollama
- Milvus collection created with document embeddings
- SQL Analytics mode querying Gold layer via Trino
- RAG mode retrieving from documents

---

## Phase 8 — Monitoring & Observability (Day 6)

### 8.1 Start Monitoring

```bash
make up-monitoring
```

**Services started:** Prometheus, Grafana, Loki, Promtail

### 8.2 Grafana Dashboards

```bash
open http://localhost:3000
# Login: admin / admin123

# Import community dashboards:
# - Kafka: Dashboard ID 7589
# - Spark: Dashboard ID 11069
# - Airflow: Dashboard ID 19518
# - Trino: Dashboard ID 16567
# - PostgreSQL: Dashboard ID 9628
```

### 8.3 Configure Alerts

In Grafana → Alerting → Alert Rules, create:
- Airflow DAG failure alert
- Kafka consumer lag > 10,000 messages
- SeaweedFS volume > 80% capacity
- Spark job runtime > 30 minutes

**✅ Phase 8 Complete Criteria:**
- All scrape targets visible in Prometheus
- Container logs streaming to Loki
- At least 4 Grafana dashboards operational
- Alert rules configured

---

## Phase 9 — Data Governance (Day 7)

### 9.1 Start Governance

```bash
make up-governance
```

**Services started:** DataHub (GMS + Frontend + Neo4j), OpenSearch, OpenBao

### 9.2 Configure DataHub

```bash
open http://localhost:9002
# Default login: datahub / datahub

# Ingest metadata from Trino:
docker exec lakehouse_datahub_gms \
    datahub ingest -c /configs/datahub/trino_recipe.yml
```

### 9.3 Configure OpenBao (Secrets)

```bash
# Initialize OpenBao
docker exec lakehouse_openbao \
    bao secrets enable kv

# Store service credentials
docker exec lakehouse_openbao \
    bao kv put kv/seaweedfs access_key=admin secret_key=admin123

docker exec lakehouse_openbao \
    bao kv put kv/postgres username=admin password=admin123
```

**✅ Phase 9 Complete Criteria:**
- DataHub showing all Iceberg tables with lineage
- OpenSearch indexing audit logs
- OpenBao storing all service secrets
- Column-level lineage from bronze → silver → gold visible

---

## Phase 10 — CI/CD & GitLab (Day 7–8)

### 10.1 Start GitLab

```bash
make up-cicd
# Allow 5-10 minutes to fully initialize
open http://localhost:8929
# Default login: root (set password on first login)
```

### 10.2 Push Codebase to GitLab

```bash
git remote add gitlab http://localhost:8929/tpl/data-lakehouse.git
git push gitlab main
```

### 10.3 CI/CD Pipeline (`.gitlab-ci.yml`)

Create `.gitlab-ci.yml` with stages:
- `lint` → dbt compile + sqlfluff
- `test` → dbt test + Great Expectations
- `deploy-dev` → dbt run on dev schema
- `deploy-prod` → dbt run on prod schema (manual gate)

**✅ Phase 10 Complete Criteria:**
- GitLab accessible with project pushed
- CI/CD pipeline running on every commit
- dbt tests passing in pipeline

---

## Airgap Transfer Checklist

For moving to an air-gapped production environment:

```bash
# 1. Pre-pull all images
make pull-images

# 2. Save to tarballs
make save-images
# Output: ./airgap-images/*.tar.gz (~25GB total)

# 3. Transfer to target machine (USB / secure file transfer)
rsync -avz ./airgap-images/ user@prod-server:/opt/lakehouse/images/

# 4. On target machine: load images
make load-images

# 5. Configure .env for production values
cp .env .env.prod
vim .env.prod

# 6. Start stack
make up-all
```

---

## Port Reference

| Service | Port | URL |
|---|---|---|
| SeaweedFS S3 | 8333 | http://localhost:8333 |
| SeaweedFS Filer | 8888 | http://localhost:8888 |
| SeaweedFS Master | 9333 | http://localhost:9333 |
| PostgreSQL | 5432 | localhost:5432 |
| Kafka | 9092 | localhost:9092 |
| Kafka UI | 9000 | http://localhost:9000 |
| Schema Registry | 8081 | http://localhost:8081 |
| Kafka Connect | 8083 | http://localhost:8083 |
| NiFi | 8090 | http://localhost:8090 |
| Spark Master UI | 8181 | http://localhost:8181 |
| Airflow | 8280 | http://localhost:8280 |
| Hive Metastore | 9083 | thrift://localhost:9083 |
| Trino | 8180 | http://localhost:8180 |
| JupyterHub | 8400 | http://localhost:8400 |
| Superset | 8500 | http://localhost:8500 |
| Ollama | 11434 | http://localhost:11434 |
| Milvus | 19530 | localhost:19530 |
| AI Chatbot | 8501 | http://localhost:8501 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |
| Loki | 3100 | http://localhost:3100 |
| DataHub | 9002 | http://localhost:9002 |
| OpenSearch | 9200 | http://localhost:9200 |
| OpenBao | 8200 | http://localhost:8200 |
| GitLab | 8929 | http://localhost:8929 |

---

## Regulatory Compliance Notes

### 21 CFR Part 11 Addressed By:
- Audit trail: OpenSearch + Airflow logs + Iceberg snapshots
- Electronic signatures: OpenBao-managed credentials
- System access control: Apache Ranger (RBAC)
- Time-sync: Docker NTP + `_ingested_at` ALCOA+ timestamps on every record

### ALCOA+ Fields on Every Bronze Record:
| Field | ALCOA+ Attribute |
|---|---|
| `_source_system` | Attributable |
| `_raw_payload` | Original |
| `_ingested_at` | Contemporaneous |
| `_row_hash` | Accurate |
| `_kafka_offset` | Enduring |
| `_ingest_year/month/day` | Available |
