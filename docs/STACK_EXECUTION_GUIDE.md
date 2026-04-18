# Stack Execution Guide

This document explains how the lakehouse stack is intended to start, how data is supposed to move through it, what appears to be working already, and what should be done next to make the platform reliably end to end.

## 1. Intended Layer Startup Order

The repo already defines the startup sequence in [start_all.ps1](/C:/Users/sharidass/Downloads/lakehouse-base-build/start_all.ps1), [docker-compose.yml](/C:/Users/sharidass/Downloads/lakehouse-base-build/docker-compose.yml), and the `Makefile`.

Recommended layer order:

1. `core`
   Start PostgreSQL and SeaweedFS.
   SeaweedFS order is `master -> volume -> filer -> s3 -> seaweedfs-init`.

2. `ingestion`
   Start `zookeeper -> kafka -> schema-registry -> kafka-connect -> kafka-ui`, plus NiFi.

3. `synthetic`
   Start the synthetic data generator so Kafka topics begin receiving events and some metadata tables are seeded in Postgres.

4. `processing`
   Start Spark master/workers, Redis, Airflow, and the helper containers for dbt, Tika, and Tesseract.

5. `lakehouse`
   Start Hive Metastore first, then Trino.

6. `monitoring`
   Start Prometheus, Grafana, Loki, Promtail, and exporters.

7. `analytics`
   Start JupyterHub, Superset, and the admin dashboard.

8. `ai`
   Start Ollama, model pull, Milvus/etcd, and the LangChain app.

9. `governance`
   Start OpenSearch, DataHub MySQL, DataHub GMS/frontend, Neo4j, and OpenBao.

10. `cicd`
    Start GitLab.

If you want the shortest path to a usable lakehouse, focus only on:

`core -> ingestion -> synthetic -> processing -> lakehouse -> analytics`

That is the minimum useful path for proving bronze, silver, gold, and SQL serving.

## 2. Intended Data Execution Order

The business/data flow is different from the container startup order.

Intended runtime flow:

1. Synthetic data or real source systems publish events into Kafka topics.
2. Spark bronze ingestion jobs read Kafka topics and land raw records into Iceberg tables on SeaweedFS.
3. Airflow orchestrates the medallion pipeline.
4. dbt models transform bronze tables into silver cleaned tables.
5. dbt builds gold marts from silver.
6. Trino exposes Iceberg schemas for query access.
7. Superset, Jupyter, and the AI app consume Trino and document/vector outputs.
8. DataHub adds metadata, lineage, and governance on top.

Key implementation files:

- [configs/airflow/dags/medallion_pipeline.py](/C:/Users/sharidass/Downloads/lakehouse-base-build/configs/airflow/dags/medallion_pipeline.py)
- [configs/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py](/C:/Users/sharidass/Downloads/lakehouse-base-build/configs/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py)
- [dbt/models/silver/sources.yml](/C:/Users/sharidass/Downloads/lakehouse-base-build/dbt/models/silver/sources.yml)
- [configs/trino/catalog/iceberg.properties](/C:/Users/sharidass/Downloads/lakehouse-base-build/configs/trino/catalog/iceberg.properties)

## 3. Current Architecture by Layer

### Core

- PostgreSQL stores metadata and backing databases for Airflow, Hive Metastore, Superset, and app metadata.
- SeaweedFS provides S3-compatible storage for bronze, silver, gold, models, docs, and Milvus support.

### Ingestion

- Kafka is the event backbone.
- Schema Registry and Kafka Connect are prepared for CDC and connector-based ingestion.
- NiFi is present for file, flow, or OT-style ingestion.
- Synthetic generator produces MES, IQMS, Historian, Trackwise, SAP, and TMS data.

### Processing

- Spark is used for bronze ingestion into Iceberg.
- Airflow is the orchestrator for medallion and maintenance flows.
- Great Expectations checkpoints are included for silver-layer quality gates.

### Lakehouse

- Hive Metastore holds Iceberg table metadata.
- SeaweedFS stores table data files.
- Trino queries Iceberg via Hive Metastore.

### Analytics

- Superset is intended for dashboards.
- JupyterHub is intended for notebooks and ad hoc analysis.
- Admin dashboard is an operational UI.

### AI

- Ollama supplies the local LLM.
- Milvus provides the vector store.
- LangChain app combines document RAG and SQL querying over Trino.

### Governance

- DataHub is intended for metadata and lineage.
- OpenSearch is intended for search/audit support.
- OpenBao is intended for secrets.

### Monitoring

- Prometheus, Grafana, Loki, Promtail, node exporter, and Postgres exporter are included.

## 4. What Looks Implemented vs Placeholder

### Looks substantially implemented

- Docker Compose layering and profiles
- SeaweedFS object storage initialization
- Kafka-based synthetic streaming
- Spark bronze ingestion pattern into Iceberg
- dbt silver and gold model structure
- Trino Iceberg catalog setup
- Monitoring stack scaffolding
- LangChain app container and utilities

### Looks partially implemented or placeholder

- `dbt` execution still depends on package installation inside Airflow at container startup
- Airflow image does not obviously include `dbt-trino`
- `tika` and `tesseract` services are placeholder Spark containers
- governance is not fully reliable until DataHub GMS is healthy
- some generated Kafka topics do not yet have matching bronze ingestion tasks

## 5. Main Gaps Blocking a Clean End-to-End Run

These are the highest-value blockers to fix before expanding the stack further.

### Blocker 1: Spark version mismatch

The repo should use one Spark line consistently. The current fix standardizes on Spark `3.4`, but any remaining references should stay aligned with that.

Why it matters:

- Spark package compatibility can break `SparkSubmitOperator` jobs.
- Iceberg and Kafka runtime jars should match the actual Spark runtime.

Suggested fix:

- Choose one Spark version and align Compose, package coordinates, and docs.
- The easiest path is to standardize the repo on one version only.

### Blocker 2: dbt execution path is inconsistent

Current situation:

- Compose now defines `dbt` as a dedicated `dbt-trino` image.
- Airflow DAG runs `dbt` commands directly inside Airflow tasks.
- Airflow depends on `dbt-trino` being installed from `_PIP_ADDITIONAL_REQUIREMENTS` at startup.

Why it matters:

- Silver and gold likely fail even if bronze succeeds.

Suggested fix:

- The immediate fix is to keep Airflow and the standalone dbt helper image on the same dbt-trino line.
- A later improvement would be moving dbt execution behind a more explicit runtime boundary instead of relying on Airflow startup package installation.

### Blocker 3: Bronze topic coverage does not fully match downstream expectations

Examples:

- `iqms.deviations` needed to be added to the bronze DAG so silver deviation models have a source table.
- dbt sources declare `iqms_deviations`, so downstream expectations exceed current bronze ingestion.
- Additional generated topics like `mes.oee_metrics`, `trackwise.complaints`, and `sap.purchase_orders` are not fully wired into bronze.

Why it matters:

- Silver and gold models may fail or remain incomplete.
- Architecture intent is larger than current executable flow.

Suggested fix:

- Reconcile topic inventory across:
  - synthetic generator
  - bronze Airflow DAG
  - dbt source declarations
  - gold marts that depend on silver outputs

### Blocker 4: Document ingestion stack is not production-ready yet

Current situation:

- `pdf_processing_pipeline.py` expects Tika and OCR behavior.
- Compose services named `tika` and `tesseract` are currently placeholder containers.

Why it matters:

- RAG document extraction/indexing flow is not trustworthy yet.

Suggested fix:

- Replace placeholder containers with real Tika and OCR images before investing more effort in document workflows.

### Blocker 5: Governance layer is not fully healthy yet

Observed state:

- Most services are running, but DataHub GMS was not visible in the live `docker compose ps` output when checked.

Why it matters:

- DataHub frontend alone is not enough for lineage ingestion and metadata operations.

Suggested fix:

- Debug GMS startup and dependency readiness after the core medallion path is stable.

## 6. Recommended Execution Roadmap

This is the most practical order for continuing the project.

### Phase A: Prove the minimum viable lakehouse

Goal:

- Get one complete bronze -> silver -> gold -> Trino -> Superset path working.

Do this first:

1. Align Spark runtime and Spark package versions.
2. Make dbt executable in a single consistent way.
3. Confirm Hive Metastore + Trino can see Iceberg.
4. Pick one domain end to end, ideally MES production orders.
5. Run:
   `Kafka topic -> bronze table -> silver model -> gold mart -> Trino query -> Superset dataset`

Definition of done:

- A Trino query against one gold mart returns expected rows sourced from synthetic Kafka events.

### Phase B: Expand bronze coverage

Goal:

- Bring generator topics and bronze ingestion into alignment.

Do next:

1. Add missing bronze tasks for required topics.
2. Update dbt source declarations to match actual landed tables.
3. Re-run silver/gold on the newly landed sources.

### Phase C: Harden orchestration and quality

Goal:

- Make the medallion pipeline repeatable and observable.

Do next:

1. Validate Airflow DAG task dependencies.
2. Verify Great Expectations checkpoints against actual silver tables.
3. Add a simple smoke-check script:
   `Kafka -> Bronze count -> Silver count -> Gold count -> Trino query`

### Phase D: Stabilize analytics

Goal:

- Make the lakehouse usable by analysts.

Do next:

1. Create Superset Trino connection and datasets.
2. Build one OEE dashboard and one compliance dashboard.
3. Validate Jupyter notebooks against Trino or Spark.

### Phase E: Add AI and governance after the data plane is stable

Goal:

- Avoid debugging AI/governance before the core tables are trustworthy.

Do later:

1. Fix Tika/Tesseract services.
2. Validate Milvus indexing flow.
3. Fix DataHub GMS startup and metadata ingestion.
4. Emit lineage only after the medallion path is consistently passing.

## 7. Suggested Immediate Next Tasks

If continuing this repo right now, the best next tasks are:

1. Normalize versions.
   Align Spark, Iceberg, and related package coordinates.

2. Validate dbt runtime.
   Confirm Airflow and the standalone dbt helper container both resolve the same dbt-trino version successfully.

3. Reconcile topic-to-table mapping.
   Build a single source-of-truth matrix for:
   topic name, bronze table, silver model, gold mart.

4. Prove one vertical slice.
   Make MES production orders the first fully working end-to-end path.

5. Only then widen scope.
   Add more domains, then AI/RAG, then governance hardening.

## 8. Short Answer: What Should Be Done Next?

The next best move is not to add more services.

The next best move is to make the existing medallion core reliable:

`Kafka -> Spark bronze -> Iceberg -> dbt silver/gold -> Trino`

Once that vertical slice is stable, the rest of the stack becomes much easier to validate and extend.
