# Enterprise Data Lakehouse - Stack Execution Guide

This document explains the runtime sequencing, data movement, architecture breakdown, and system readiness of the Enterprise Data Lakehouse stack. The platform has been fully audited, cleaned, and deployed, representing a reliable end-to-end data environment.

---

## 1. Execution Layer Startup Order

The platform relies on a distributed microservice architecture. Startup sequencing is strictly managed via Docker Compose depends-on conditions, but logic flows sequentially through these layers.

If you are using the automated startup script (`start_all.ps1` or `start_all.sh`), the layers launch in this order:

1. **`core` Layer**
   Starts PostgreSQL (Metastore/Management) and SeaweedFS (S3 backend).
2. **`ingestion` Layer**
   Starts the Event Backbone (`zookeeper -> kafka -> schema-registry -> kafka-connect`) and Apache NiFi.
3. **`synthetic` Layer**
   Starts the Python Synthetic Data Generator to stream raw business events (MES, SAP, IQMS) into Kafka.
4. **`processing` Layer**
   Starts Apache Spark master/workers, Redis, Airflow orchestrator, and helper containers (dbt, Apache Tika).
5. **`lakehouse` Layer**
   Starts Hive Metastore, followed instantly by the Trino SQL Engine.
6. **`monitoring` Layer**
   Starts Prometheus, Grafana, Loki, Promtail, and Node/DB Exporters.
7. **`analytics` Layer**
   Starts JupyterHub, Apache Superset, and the Streamlit Admin Operational Dashboard.
8. **`ai` Layer**
   Starts Ollama LLM, pulls the Llama 3 model, deploys the Milvus Vector database, and launches the LangChain RAG Chat app.
9. **`governance` Layer**
   Starts OpenSearch, DataHub backends (MySQL/GMS), Neo4j graph, and OpenBao for secrets.
10. **`cicd` Layer**
    Starts GitLab CE for source control.

---

## 2. Intended Data Execution Order

The business data flows in a completely synchronized Medallion architecture:

1. **Ingestion**: Synthetic data or real source systems publish raw events into structured Kafka topics.
2. **Bronze Land**: Spark `SparkSubmitOperator` ingestion jobs read Kafka topics and convert raw payloads into native Iceberg tables on SeaweedFS.
3. **Orchestration**: Apache Airflow operates the scheduled Medallion Pipeline DAG.
4. **Silver Prep**: `dbt Core` transforms Bronze Iceberg tables into clean, standardized, and filtered Silver tables.
5. **Gold Marts**: `dbt Core` builds highly-aggregated Gold business marts (e.g., OEE calculations, Quality Risk, CAPA distribution).
6. **Serving**: Trino exposes the finalized Iceberg target schemas for unified SQL access.
7. **Consumption**: Apache Superset and Grafana consume Trino tables to render dynamic business and operational dashboards.
8. **AI**: The Langchain interface relies on Trino data to answer tabular queries via SQL-generation, and Milvus metadata to execute Document Retrieval-Augmented Generation (RAG).
9. **Governance**: DataHub passively ingests lineage map schemas on top of the entire flow.

---

## 3. Current Architecture by Layer

### Core Storage
- **PostgreSQL**: Stores backend application states for Airflow, Hive, Superset, and DataHub.
- **SeaweedFS**: S3-compatible, horizontally scalable storage for all Bronze/Silver/Gold table properties, documents, and vectors.

### Ingestion
- **Kafka**: Real-time event transport.
- **Apache NiFi**: Flow-based extraction.
- **Synthetic Data**: Continuously simulates manufacturing traffic mapped cleanly to Kafka topics without requiring live plant integrations.

### Processing
- **Apache Spark**: Executes Iceberg dataframe operations.
- **Apache Airflow**: Tracks Medallion checkpoints, compaction, and DAG dependencies.
- **dbt**: Orchestrated through Airflow for SQL transformations using dynamic trino adaptors.

### Lakehouse
- **Hive Metastore**: Maps data schemas.
- **Trino**: Extremely fast distributed SQL query engine mapped natively to the `iceberg` catalog.

### Analytics
- **Apache Superset**: Business Intelligence.
- **Grafana**: Built with Trino native query adaptations to observe Cross-Database metrics securely.

### Generative AI
- **Ollama**: Ensures an airgapped execution of Large Language Models (Llama 3).
- **Milvus**: Stores high-dimensional vector embeddings for regulatory document similarity searches.
- **LangChain**: Ties RAG and Text-To-SQL together inside a streamlined application.

### Governance
- **DataHub**: Implements enterprise glossaries, access mappings, and automated pipeline lineage.
- **OpenSearch**: Supports heavy logging infrastructure.

---

## 4. System Readiness & Validation

The entire Data Lakehouse platform has been **validated for end-to-end operational capacity**. 

### ✅ Functional Confirmations
- **Medallion Pipeline**: The continuous streaming path from `Kafka -> Spark Bronze -> dbt Silver -> dbt Gold` successfully operates inside Airflow without dependency drift or package failures.
- **Grafana Data Cross-Linking**: Grafana visually extracts both Prometheus logs and native Trino SQL metrics side-by-side using unified data connections. No `pq: cross-database` errors exist.
- **Schema Sanitization**: Environment holds zero external references, hardcoded client metadata (e.g., legacy TPL markers), or structural anomalies.
- **RAG Execution**: Tika, Ollama, and Langchain modules map cleanly, supporting safe, offline intelligence deployments.

## 5. Standard Operating Procedures (Runbooks)

If maintaining or developing further on this stack, adhere to the following processes:

1. **Modifying Dashboards**:
   - Always map application health metrics to the `Prometheus` and `Loki` datasets.
   - Always map aggregations, layer count diagnostics, or business KPIs directly to the `Trino` dataset representing the `iceberg.gold` or `iceberg.silver` schema bounds.

2. **Recompiling Transformations**:
   - Any new Kafka topics **must** be wired into `configs/airflow/dags/spark_jobs/` first to achieve Bronze status.
   - Afterwards, apply strict YAML configurations in `dbt/models/silver/sources.yml`. Superset will instantly be aware of new Gold derivations once DAGs have completed.

3. **Routine Maintenance**:
   - Airflow includes DAGs specifically designated for Iceberg cleanup (`VACUUM` equivalents). Leave these active to prevent SeaweedFS data explosion from metadata retention issues.
