# Client Demo Guide

This guide is a practical walkthrough for presenting the lakehouse stack to a client or reviewer. It focuses on three use cases:

1. BI lakehouse: data moves from source events to Gold marts and Superset dashboards.
2. RAG assistant: local documents and lakehouse context become searchable through a LangChain/Ollama app.
3. Operations and governance: Grafana, Prometheus, DataHub, and OpenBao show the platform is controllable.

The demo should feel like a working data product, not a tool inventory. The story is: "We can ingest operational data, govern it, transform it into trusted analytics, and use it through dashboards and AI."

## Demo Setup

Start the full stack:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_all.ps1
```

Or start from Bash:

```bash
bash start_all.sh
```

Check health:

```bash
docker compose --env-file .env --profile all ps
bash check_all.sh
```

If time is limited, focus on these URLs:

| Purpose | URL | Login |
| --- | --- | --- |
| Kafka topics | [http://localhost:9000](http://localhost:9000) | none |
| Airflow orchestration | [http://localhost:8280](http://localhost:8280) | `admin / admin` |
| Trino SQL | [http://localhost:8180](http://localhost:8180) | `admin` |
| Superset BI | [http://localhost:8500](http://localhost:8500) | `admin / admin` |
| RAG app | [http://localhost:8501](http://localhost:8501) | none |
| Grafana | [http://localhost:3000](http://localhost:3000) | `admin / admin123` |
| DataHub | [http://localhost:9002](http://localhost:9002) | `datahub / datahub` |

## Opening Talk Track

"This is a local enterprise lakehouse stack. It simulates manufacturing and quality data, lands it into a medallion architecture, transforms it into BI-ready marts, and then exposes it through Superset dashboards and a local RAG assistant. The stack also includes monitoring and governance so the data product can be operated, not just queried."

## Architecture to Explain

```text
MES, IQMS, SAP, Trackwise, Historian, TMS
  -> Kafka / NiFi
  -> Bronze Iceberg tables on SeaweedFS
  -> Spark, Airflow, dbt, Great Expectations
  -> Silver models
  -> Gold marts
  -> Trino
  -> Superset BI, Grafana operations, LangChain RAG
  -> DataHub governance
```

Use the "house" explanation:

- Foundation: Docker Compose, PostgreSQL, SeaweedFS, Kafka, Spark, Hive Metastore, Trino.
- Storage rooms: Bronze, Silver, and Gold.
- Workshop: Airflow, Spark, dbt, Great Expectations.
- Front office: Superset, Grafana, JupyterHub, DataHub.
- Study room: Ollama, Milvus, Tika, and LangChain.

## Use Case 1: BI Lakehouse

### Business Message

"The BI path proves that operational events can become trusted business KPIs. Raw manufacturing, quality, inventory, training, and CAPA data flows through Bronze, is standardized in Silver, and becomes dashboard-ready in Gold."

### What to Show

1. Open Kafka UI: [http://localhost:9000](http://localhost:9000)
2. Show topics such as:
   - `mes.production_orders`
   - `mes.machine_status`
   - `mes.oee_metrics`
   - `iqms.quality_tests`
   - `iqms.deviations`
   - `sap.inventory_movements`
   - `trackwise.capas`
   - `tms.training_completions`
3. Open Airflow: [http://localhost:8280](http://localhost:8280)
4. Show the medallion pipeline DAG.
5. Open Trino and query Gold tables.
6. Open Superset and show BI dashboards or datasets.

### Commands to Run

List topics:

```bash
make kafka-topics
```

Open Trino shell:

```bash
make trino-shell
```

Run:

```sql
SHOW TABLES FROM iceberg.gold;
SELECT * FROM iceberg.gold.gold_oee_dashboard LIMIT 10;
SELECT * FROM iceberg.gold.gold_quality_kpis LIMIT 10;
```

Run the full demo pipeline:

```bash
bash scripts/run_full_demo.sh
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_full_demo.ps1
```

### Key Files to Show

| File | Why It Matters |
| --- | --- |
| `synthetic_data/generator.py` | Creates realistic source-system events. |
| `configs/airflow/dags/medallion_pipeline.py` | Orchestrates the data pipeline. |
| `configs/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py` | Moves Kafka data into Bronze Iceberg. |
| `dbt/models/silver/` | Cleans and standardizes source data. |
| `dbt/models/gold/` | Builds dashboard-ready business marts. |
| `scripts/setup_superset.py` | Provisions Superset assets. |

### BI Talk Track

"Kafka shows the live operational signal. Airflow shows that the pipeline is controlled and repeatable. dbt shows that transformations are versioned. Trino gives one SQL access layer across the lakehouse. Superset turns the Gold layer into business-facing BI."

### Success Criteria

- Kafka topics contain messages.
- Airflow pipeline tasks are healthy.
- Trino can query `iceberg.gold`.
- Superset can show datasets or dashboards from Gold marts.

## Use Case 2: RAG Assistant

### Business Message

"The RAG path lets users ask questions against local project knowledge and, when enabled, lakehouse tables. It is designed for airgapped or on-prem environments because the LLM, embeddings, vector database, and document parsing run locally."

### What to Show

1. Open the RAG app: [http://localhost:8501](http://localhost:8501)
2. Explain the local AI services:
   - Ollama runs the chat model.
   - Milvus stores vectors.
   - Tika extracts text.
   - LangChain orchestrates retrieval and responses.
3. Show the mounted knowledge sources:
   - `README.md`
   - `docs/`
   - `data/source/`
4. Ask a stack-related question.
5. Ask a BI/data question if Trino tools are available.

### Example Questions

```text
What are the main layers in this lakehouse stack?
How does the BI flow work from Bronze to Superset?
Which services support RAG in this environment?
What Gold tables are useful for quality and manufacturing KPIs?
How would I check whether Trino and Superset are healthy?
```

### Commands to Run

Check Ollama:

```bash
docker exec lakehouse_ollama ollama list
```

Check Milvus:

```bash
curl http://localhost:9091/healthz
```

Check app logs:

```bash
docker logs lakehouse_langchain_app --tail 100
```

### Key Files to Show

| File | Why It Matters |
| --- | --- |
| `dockerfiles/langchain-app/app.py` | Streamlit UI and chat flow. |
| `dockerfiles/langchain-app/utils/document_qa.py` | Retrieval and document QA logic. |
| `dockerfiles/langchain-app/utils/index_documents.py` | Document indexing into Milvus. |
| `dockerfiles/langchain-app/utils/trino_tool.py` | Optional SQL helper for lakehouse queries. |
| `docker-compose.yml` | Shows Ollama, Milvus, Tika, and app wiring. |

### RAG Talk Track

"This is not calling a hosted AI service. The model is local, the embeddings are local, and the vector database is local. That makes it suitable for secure environments where documents and manufacturing data cannot leave the network."

### Success Criteria

- Ollama responds.
- Milvus health endpoint is healthy.
- LangChain app loads.
- The app can answer from the project documentation or indexed files.

## Use Case 3: Operations and Governance

### Business Message

"A data platform is only useful if it can be operated and governed. This stack includes health monitoring, logs, metadata discovery, lineage, and secrets-management demo capability."

### What to Show

1. Open Grafana: [http://localhost:3000](http://localhost:3000)
2. Show provisioned dashboards for pipeline health, manufacturing OEE, or compliance/audit.
3. Open Prometheus: [http://localhost:9090](http://localhost:9090)
4. Show active targets.
5. Open DataHub: [http://localhost:9002](http://localhost:9002)
6. Explain metadata ingestion from Trino, dbt, and Airflow recipes.
7. Optionally open OpenBao: [http://localhost:8200](http://localhost:8200)

### Commands to Run

Prometheus:

```bash
curl http://localhost:9090/-/healthy
```

Grafana:

```bash
curl http://localhost:3000/api/health
```

Loki:

```bash
curl http://localhost:3100/ready
```

DataHub:

```bash
curl http://localhost:8880/health
```

### Key Files to Show

| File | Why It Matters |
| --- | --- |
| `configs/prometheus/prometheus.yml` | Defines service scrape targets. |
| `configs/grafana/provisioning/datasources/datasources.yml` | Pre-wires Grafana to Prometheus, Loki, and Trino. |
| `configs/grafana/provisioning/dashboards/` | Stores dashboard definitions as code. |
| `configs/datahub/trino_recipe.yml` | Metadata ingestion from Trino. |
| `configs/datahub/dbt_recipe.yml` | Metadata and lineage from dbt. |
| `configs/datahub/airflow_recipe.yml` | Pipeline metadata from Airflow. |

### Operations Talk Track

"Grafana tells us whether the platform is healthy. Prometheus collects metrics. Loki keeps logs searchable. DataHub tells users what data exists, where it came from, and how it flows through the lakehouse."

### Success Criteria

- Prometheus targets are up.
- Grafana API is healthy.
- Loki is ready.
- DataHub UI loads.
- The governance story is tied back to Trino, dbt, and Airflow metadata.

## Suggested Demo Flow

Use this order for a clean 20-30 minute walkthrough:

1. Architecture overview.
2. Kafka UI to show live source events.
3. Airflow to show orchestration.
4. Trino to show SQL access to Gold.
5. Superset to show BI consumption.
6. LangChain app to show RAG.
7. Grafana to show platform health.
8. DataHub to close with governance.

## Short Version for Executives

"The stack takes live or synthetic manufacturing data, lands it in a lakehouse, cleans it, models it, and serves it through dashboards and AI. It also includes the operational and governance layers needed to run it responsibly."

## Technical Close

"Everything here is containerized. The same repository defines the services, configs, transformations, dashboards, RAG app, and runbooks. For a real deployment, the next step would be replacing synthetic sources with real connectors, tightening identity and secrets, exporting final dashboards, and adding environment-specific CI/CD."

## Troubleshooting During a Demo

If something fails, use these checks:

```bash
docker compose --env-file .env --profile all ps
docker logs lakehouse_trino --tail 100
docker logs lakehouse_airflow_web --tail 100
docker logs lakehouse_langchain_app --tail 100
```

Fast fallback:

- If Superset is slow, query Gold tables directly in Trino.
- If RAG is slow, show the LangChain code and Ollama/Milvus health.
- If governance is slow, explain the recipes in `configs/datahub/`.
- If full stack is too heavy, focus on BI and RAG profiles only.
