# Stack Execution Guide

This guide explains how to start, validate, operate, and troubleshoot the local Enterprise Data Lakehouse stack. It reflects the current repository after cleanup: Docker Compose profiles, SeaweedFS object storage, Iceberg tables, Trino SQL, Superset BI, LangChain RAG, DataHub governance, and Grafana monitoring.

## Purpose

Use this document when you need to run the platform from a clean machine, bring up only part of the stack, verify service health, or explain the runtime order to another engineer.

## Quick Start

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_all.ps1
.\check_all.ps1
```

Linux or macOS:

```bash
bash start_all.sh
bash check_all.sh
```

Direct Compose status:

```bash
docker compose --env-file .env --profile all ps
```

## Startup Order

The stack is split into profiles so heavy services can be started in a controlled order.

| Order | Profile | What Starts | Why It Starts Here |
| --- | --- | --- | --- |
| 1 | `core` | PostgreSQL, SeaweedFS, S3 gateway, bucket init | Shared database and object storage must exist first. |
| 2 | `ingestion` | Zookeeper, Kafka, Schema Registry, Kafka Connect, Kafka UI, NiFi | Source events and file flows need the backbone online. |
| 3 | `synthetic` | Synthetic data generator | Feeds realistic MES, IQMS, SAP, Trackwise, Historian, and TMS events. |
| 4 | `processing` | Spark, Airflow, Redis, dbt, Great Expectations, Tika | Runs ingestion jobs, transformations, checks, and document extraction. |
| 5 | `lakehouse` | Hive Metastore, Trino | Publishes Iceberg tables through SQL. |
| 6 | `monitoring` | Prometheus, Grafana, Loki, Promtail, exporters | Adds metrics, logs, dashboards, and health visibility. |
| 7 | `analytics` | Superset, JupyterHub, admin dashboard | Enables BI, notebooks, and operational views. |
| 8 | `ai` | Ollama, Milvus, Attu, LangChain app | Enables local LLM, vector search, and RAG chat. |
| 9 | `governance` | DataHub, OpenSearch, Neo4j, OpenBao | Adds metadata, lineage, search, and secrets demo capability. |
| 10 | `cicd` | GitLab CE | Optional local CI/CD surface. |

## Manual Profile Commands

```bash
make up-core
make init-buckets
make up-ingestion
make up-synthetic
make up-processing
make up-lakehouse
make up-monitoring
make up-analytics
make up-ai
make up-governance
make up-cicd
```

Stop containers without deleting volumes:

```bash
make down
```

Stop containers and remove volumes:

```bash
make clean
```

Use `make clean` only when you want a fresh local environment.

## Data Flow

```text
Synthetic/source systems
  -> Kafka, Kafka Connect, NiFi
  -> Bronze Iceberg tables on SeaweedFS
  -> Spark and dbt transformations
  -> Silver conformed models
  -> Gold BI marts
  -> Trino SQL
  -> Superset, Grafana, JupyterHub, LangChain
```

Bronze stores raw or lightly structured records. Silver standardizes and cleans them. Gold creates dashboard-ready marts for manufacturing OEE, quality, inventory, training compliance, CAPA, and batch summaries.

## BI Runtime Path

BI depends mainly on these services:

- `core`
- `ingestion`
- `processing`
- `lakehouse`
- `analytics`

BI path:

```text
Kafka/NiFi -> Bronze -> Silver -> Gold -> Trino -> Superset
```

Important BI objects:

- Gold dbt models in `dbt/models/gold/`
- Superset setup helper in `scripts/setup_superset.py`
- Trino catalog config in `configs/trino/catalog/iceberg.properties`

Superset URL:

[http://localhost:8500](http://localhost:8500)

Default login:

```text
admin / admin
```

## RAG Runtime Path

RAG depends mainly on these services:

- `core`
- `processing`
- `lakehouse`
- `ai`

RAG path:

```text
Mounted docs/data -> Tika -> embeddings -> Milvus -> LangChain -> Ollama
```

Important RAG files:

- Streamlit app: `dockerfiles/langchain-app/app.py`
- Document QA: `dockerfiles/langchain-app/utils/document_qa.py`
- Indexing utility: `dockerfiles/langchain-app/utils/index_documents.py`
- Trino helper: `dockerfiles/langchain-app/utils/trino_tool.py`

LangChain app URL:

[http://localhost:8501](http://localhost:8501)

Default models:

```text
CHAT_MODEL=llama3
EMBEDDING_MODEL=nomic-embed-text
```

## Governance and Monitoring Path

Governance path:

```text
dbt, Trino, Airflow metadata -> DataHub -> OpenSearch/Neo4j
```

Monitoring path:

```text
Services -> Prometheus/Loki -> Grafana dashboards
```

Important files:

- DataHub recipes: `configs/datahub/`
- Grafana dashboards: `configs/grafana/provisioning/dashboards/`
- Prometheus targets: `configs/prometheus/prometheus.yml`
- Loki config: `configs/loki/loki-config.yml`
- Promtail config: `configs/promtail/promtail-config.yml`

## Core URLs

| Service | URL | Login |
| --- | --- | --- |
| Airflow | [http://localhost:8280](http://localhost:8280) | `admin / admin` |
| Trino | [http://localhost:8180](http://localhost:8180) | `admin`, no password |
| Superset | [http://localhost:8500](http://localhost:8500) | `admin / admin` |
| LangChain RAG | [http://localhost:8501](http://localhost:8501) | none |
| Admin dashboard | [http://localhost:8502](http://localhost:8502) | none |
| Grafana | [http://localhost:3000](http://localhost:3000) | `admin / admin123` |
| Prometheus | [http://localhost:9090](http://localhost:9090) | none |
| Kafka UI | [http://localhost:9000](http://localhost:9000) | none |
| NiFi | [http://localhost:8090](http://localhost:8090) | `admin / adminadminadmin` |
| DataHub | [http://localhost:9002](http://localhost:9002) | `datahub / datahub` |
| SeaweedFS S3 | [http://localhost:8333](http://localhost:8333) | `admin / admin123` |
| SeaweedFS browser | [http://localhost:8888](http://localhost:8888) | S3 credentials |
| OpenBao | [http://localhost:8200](http://localhost:8200) | token `roottoken` |
| Attu for Milvus | [http://localhost:8000](http://localhost:8000) | none |
| GitLab | [http://localhost:8929](http://localhost:8929) | root password from logs |

## Health Checks

Full quick check:

```bash
bash check_all.sh
```

PowerShell:

```powershell
.\check_all.ps1
```

Targeted checks:

```bash
curl http://localhost:9333/cluster/status
curl http://localhost:8180/v1/info
curl http://localhost:8280/health
curl http://localhost:8500/health
curl http://localhost:9090/-/healthy
curl http://localhost:3000/api/health
curl http://localhost:9091/healthz
```

## Trino Validation

```bash
make trino-shell
```

Useful SQL:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;
SELECT * FROM iceberg.gold.gold_oee_dashboard LIMIT 10;
```

## Demo Pipeline

Run the manual demo:

```bash
bash scripts/run_full_demo.sh
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_full_demo.ps1
```

This runs source generation, Bronze verification, dbt Silver, dbt tests, Great Expectations validation, dbt Gold, and final summary checks.

## Troubleshooting

If a service is unhealthy:

```bash
docker compose --env-file .env --profile all ps
docker logs <container-name> --tail 100
```

If Trino cannot query Iceberg:

1. Confirm SeaweedFS S3 is healthy.
2. Confirm Hive Metastore is running.
3. Check `configs/trino/catalog/iceberg.properties`.
4. Run `SHOW CATALOGS;` in Trino.

If Superset has no data:

1. Confirm Trino is healthy.
2. Confirm Gold tables exist.
3. Re-run dbt Gold models.
4. Run `python scripts/setup_superset.py` if datasets/charts need provisioning.

If RAG does not answer from documents:

1. Confirm Ollama is running.
2. Confirm the models exist with `docker exec lakehouse_ollama ollama list`.
3. Confirm Milvus health at `http://localhost:9091/healthz`.
4. Re-run the document indexer.

If the machine is slow:

1. Start only the profiles needed for the use case.
2. Avoid `cicd` and `governance` for lightweight BI demos.
3. Use `make down` instead of `make clean` when preserving state.
