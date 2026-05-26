# Implementation Plan

This plan describes the current lakehouse build and the recommended path for improving it. It is written for maintainers who need to understand what is already implemented, what each part is responsible for, and what should be hardened next.

## Current Architecture

```text
Sources and synthetic events
  -> Kafka, Kafka Connect, NiFi
  -> SeaweedFS object storage
  -> Iceberg Bronze tables
  -> Spark, Airflow, dbt, Great Expectations
  -> Silver conformed models
  -> Gold analytics marts
  -> Trino SQL
  -> Superset BI, Grafana operations, Jupyter analysis, LangChain RAG
  -> DataHub governance and lineage
```

## Current Capabilities

| Area | Implemented Capability |
| --- | --- |
| Local runtime | Docker Compose profiles for core, ingestion, synthetic, processing, lakehouse, analytics, AI, monitoring, governance, and CI/CD. |
| Storage | SeaweedFS S3-compatible storage for lakehouse buckets, documents, model artifacts, and Milvus backing storage. |
| Table format | Apache Iceberg through Hive Metastore and Trino catalog configuration. |
| Ingestion | Kafka, Kafka Connect, NiFi, synthetic manufacturing data, source generation scripts, and Bronze load helpers. |
| Processing | Spark jobs, Airflow DAGs, dbt transformations, and Great Expectations checks. |
| BI | Gold marts exposed through Trino and consumable by Superset. |
| RAG | LangChain Streamlit app using Ollama, Milvus, Tika, document indexing, and optional Trino query tools. |
| Governance | DataHub recipes, glossary configuration, lineage helper scripts, OpenSearch, Neo4j, and OpenBao. |
| Observability | Prometheus, Grafana, Loki, Promtail, node exporter, PostgreSQL exporter, and dashboard provisioning. |
| Developer workflow | Make targets, startup scripts, health checks, integration test script, and runbook docs. |

## Primary Use Cases

### Use Case 1: BI Lakehouse

Goal: show a complete analytics flow from operational events to dashboard-ready marts.

Path:

```text
Synthetic or source data -> Kafka/NiFi -> Bronze -> Silver -> Gold -> Trino -> Superset
```

Main files:

- `synthetic_data/generator.py`
- `scripts/create_bronze_tables.py`
- `configs/airflow/dags/medallion_pipeline.py`
- `dbt/models/silver/`
- `dbt/models/gold/`
- `scripts/setup_superset.py`

Definition of done:

- Kafka topics receive events.
- Bronze tables exist in Iceberg.
- Silver and Gold dbt models run successfully.
- Superset can query Gold tables through Trino.
- At least one business dashboard is available for OEE, quality, inventory, training, or CAPA.

### Use Case 2: RAG and Data Assistant

Goal: provide a local AI assistant that can answer from project documents and, where enabled, query lakehouse tables.

Path:

```text
Documents -> Tika -> embeddings -> Milvus -> LangChain app -> Ollama
Gold/Silver tables -> Trino helper -> LangChain app
```

Main files:

- `dockerfiles/langchain-app/app.py`
- `dockerfiles/langchain-app/utils/document_qa.py`
- `dockerfiles/langchain-app/utils/index_documents.py`
- `dockerfiles/langchain-app/utils/trino_tool.py`
- `dockerfiles/langchain-app/requirements.txt`

Definition of done:

- Ollama has the chat and embedding models.
- Milvus is healthy.
- Documents can be indexed.
- The Streamlit app responds through RAG.
- Trino-backed questions can query known lakehouse tables when enabled.

### Use Case 3: Operations, Governance, and Demo Control

Goal: prove the platform is observable, governable, and explainable.

Path:

```text
Services -> Prometheus/Loki -> Grafana
dbt/Airflow/Trino metadata -> DataHub
credentials/demo secrets -> OpenBao
```

Main files:

- `configs/prometheus/prometheus.yml`
- `configs/grafana/provisioning/`
- `configs/loki/loki-config.yml`
- `configs/promtail/promtail-config.yml`
- `configs/datahub/`
- `configs/airflow/dags/datahub_lineage/emit_lineage.py`

Definition of done:

- Grafana dashboards load.
- Prometheus targets are up.
- Loki is ready.
- DataHub can ingest core metadata.
- Lineage story is clear from source to Gold.

## Implementation Phases

### Phase 1: Baseline Runtime

Status: implemented.

Scope:

- Docker Compose network and profiles.
- PostgreSQL shared metadata database.
- SeaweedFS master, volume, filer, and S3 gateway.
- Bucket initialization script.

Validation:

```bash
make up-core
make init-buckets
curl http://localhost:9333/cluster/status
```

### Phase 2: Ingestion

Status: implemented.

Scope:

- Kafka and Zookeeper.
- Schema Registry.
- Kafka Connect.
- Kafka UI.
- NiFi.
- Synthetic data generator.

Validation:

```bash
make up-ingestion
make up-synthetic
make kafka-topics
docker logs lakehouse_synthetic_datagen --tail 50
```

### Phase 3: Lakehouse Tables

Status: implemented with room for hardening.

Scope:

- Bronze creation/loading helpers.
- Spark and Iceberg configuration.
- Hive Metastore.
- Trino Iceberg catalog.

Validation:

```bash
make up-processing
make up-lakehouse
make trino-shell
```

SQL:

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
```

### Phase 4: Silver and Gold Transformations

Status: implemented.

Scope:

- dbt project.
- Silver source models.
- Gold marts.
- dbt tests.
- Great Expectations checkpoints.

Validation:

```bash
make dbt-run
make dbt-test
```

Recommended next improvements:

- Add more source freshness checks.
- Add row-count reconciliation between Bronze and Silver.
- Add KPI-specific acceptance thresholds for Gold models.

### Phase 5: BI

Status: implemented.

Scope:

- Superset container.
- Trino database connection.
- Chart/dashboard setup helper.
- Gold marts for analytics.

Validation:

```bash
curl http://localhost:8500/health
python scripts/setup_superset.py
```

Recommended next improvements:

- Export final dashboards as versioned assets.
- Add dashboard screenshots to docs.
- Add role-based Superset examples for business and admin users.

### Phase 6: RAG

Status: implemented.

Scope:

- Ollama.
- Milvus.
- Attu.
- Tika.
- LangChain app.
- Document indexing and QA utilities.
- Trino query helper.

Validation:

```bash
curl http://localhost:9091/healthz
docker exec lakehouse_ollama ollama list
docker logs lakehouse_langchain_app --tail 100
```

Recommended next improvements:

- Add a sample document corpus under `docs/sample-rag/`.
- Add an automated smoke test for indexing and retrieval.
- Add citation display in the Streamlit UI for retrieved chunks.

### Phase 7: Governance

Status: implemented as a local demo layer.

Scope:

- DataHub GMS and frontend.
- OpenSearch.
- Neo4j.
- OpenBao.
- DataHub recipes.
- Glossary and lineage helpers.

Validation:

```bash
make up-governance
curl http://localhost:9002
curl http://localhost:8880/health
```

Recommended next improvements:

- Add one-click metadata ingestion command.
- Add screenshots or expected DataHub entities.
- Map glossary terms to Gold BI marts.

### Phase 8: Observability

Status: implemented.

Scope:

- Prometheus scrape configuration.
- Grafana datasources and dashboards.
- Loki and Promtail.
- Exporters.

Validation:

```bash
make up-monitoring
curl http://localhost:9090/-/healthy
curl http://localhost:3000/api/health
curl http://localhost:3100/ready
```

Recommended next improvements:

- Add alert rules for Airflow failures, Kafka lag, Trino failure rate, and storage pressure.
- Add dashboard screenshots to the demo guide.
- Add service-level objectives for demo readiness.

## Hardening Backlog

| Priority | Item | Reason |
| --- | --- | --- |
| High | Add sample data fixtures for deterministic demo runs. | Makes demos repeatable. |
| High | Add smoke test script for BI path. | Confirms Bronze to Superset readiness quickly. |
| High | Add smoke test script for RAG path. | Confirms document indexing and chat readiness quickly. |
| Medium | Version Superset dashboard exports. | Keeps BI assets portable. |
| Medium | Add DataHub ingestion Make targets. | Simplifies governance demo setup. |
| Medium | Add environment profiles for lightweight demo vs full stack. | Helps smaller machines. |
| Low | Add GitLab CI pipeline template. | Useful only when local GitLab is part of the demo. |

## Operational Rules

- Keep generated outputs out of Git.
- Keep `.env` local and never commit credentials.
- Use `make down` for normal shutdown.
- Use `make clean` only when a full volume reset is intended.
- Validate Compose after editing `docker-compose.yml`.
- Compile Python after editing scripts or app code.
- Keep documentation aligned with actual service names and ports.

## Release Checklist

Before handing the stack to another user:

```bash
git status --short
docker compose --env-file .env --profile all config --quiet
python -m compileall -q scripts synthetic_data configs dockerfiles
```

Then verify:

- README is current.
- `docs/STACK_EXECUTION_GUIDE.md` explains how to run the stack.
- `docs/IMPLEMENTATION_PLAN.md` explains what is built and what remains.
- `docs/TPL_DATA_LAKEHOUSE_CLIENT_DEMO.md` explains how to demo BI, RAG, and operations.
