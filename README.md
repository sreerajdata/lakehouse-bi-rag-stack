# Enterprise End-to-End Data Lakehouse Stack

A production-ready, airgapped Enterprise Data Lakehouse stack built on Docker Compose, featuring a complete Medallion architecture, AI/ML RAG pipeline, and comprehensive governance.

## Architecture Overview

This stack implements a unified data platform including:

*   **Storage Layer**: SeaweedFS (S3-compatible distributed storage)
*   **Metadata & Catalog**: Apache Hive Metastore + Apache Iceberg
*   **Query Engine**: Trino (Distributed SQL)
*   **Processing Layer**: Apache Spark + Apache Airflow + dbt
*   **Ingestion Layer**: Apache Kafka + NiFi + CDC (Debezium)
*   **AI Layer**: Ollama (LLM) + Milvus (Vector DB) + LangChain
*   **Governance**: DataHub (Metadata/Lineage) + OpenSearch + OpenBao (Vault)
*   **Monitoring**: Prometheus + Grafana + Loki

## Infrastructure Highlights

*   **Medallion Architecture**: Automated bronze/silver/gold tier transitions via Spark and dbt.
*   **Stabilized Services**: Hardened Hive Metastore with embedded S3 drivers, optimized DataHub-MySQL connectivity, and synchronized Milvus startup.
*   **Airgap Ready**: Infrastructure designed for on-premises deployment with pre-configured container images and localized health checks.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Windows (PowerShell) or Linux (Bash)

### Startup Sequence
To start the entire stack in the correct dependency order, run the provided startup script:

**PowerShell (Windows):**
```powershell
powershell -ExecutionPolicy Bypass -File start_all.ps1
```

**Bash (Linux):**
```bash
bash start_all.sh
```

## Service Access
| Service | Link |
|---|---|
| **DataHub** | http://localhost:9002 |
| **Airflow** | http://localhost:8280 |
| **Trino** | http://localhost:8180 |
| **JupyterHub** | http://localhost:8400 |
| **Grafana** | http://localhost:3000 |

---
*Developed for robust enterprise data engineering and AI applications.*
