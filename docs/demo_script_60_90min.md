# Enterprise Lakehouse Demo Script

## 60–90 minute presenter guide

This guide follows the current Docker Compose implementation and covers batch, streaming, SeaweedFS/Iceberg, Spark, dbt, Trino, Superset, JupyterLab, Airflow, observability, governance, and the separate RAG/SQL assistant.

Use the Say sections as presenter notes. Run commands in order and prove each transition with a count, timestamp, query, or UI view.

---

## 0. Presenter guardrails

The current stack has a reliable manual path, but do not overstate these items:

- Do not claim automatic orchestration if the Airflow scheduler is restarting.
- Do not claim Great Expectations passed if its import mismatch is still present.
- Do not claim fresh streaming ingestion unless Kafka and Bronze counts increase during the demo.
- Do not claim exactly-once delivery. Say “offset-aware, deduplicated Bronze ingestion.”
- Do not claim the OEE notebook cell works until its SQL matches the current Gold schema.
- Do not claim RAG is ready until Milvus has a complete index and a question returns an answer with sources.

The core flow is:

    Batch files / Kafka events
      -> NiFi and/or Kafka
      -> Spark ingestion
      -> SeaweedFS-backed Iceberg Bronze
      -> dbt Silver
      -> dbt Gold
      -> Trino
      -> Superset / Jupyter / SQL assistant

The separate document path is:

    Documents -> SeaweedFS -> Tika -> Ollama embeddings -> Milvus -> LangChain RAG

---

## 1. Before the call

### Open these tabs

| Layer | URL | Purpose |
|---|---|---|
| SeaweedFS | http://localhost:8888 | Object storage |
| NiFi | http://localhost:8090/nifi | Ingestion flows |
| Kafka UI | http://localhost:9000 | Topics and messages |
| Spark UI | http://localhost:8181 | Compute |
| Trino | http://localhost:8180 | SQL |
| Airflow | http://localhost:8280 | Orchestration |
| Superset | http://localhost:8500 | BI |
| JupyterLab | http://localhost:8400 | Notebook analytics |
| RAG app | http://localhost:8501 | AI assistant |
| Grafana | http://localhost:3000 | Monitoring |
| DataHub | http://localhost:9002 | Governance |
| Admin dashboard | http://localhost:8502 | Operations |

Credentials:

    Superset: admin / admin
    Airflow: admin / admin
    Grafana: admin / admin123
    DataHub: datahub / datahub
    SeaweedFS S3: admin / admin123

### Prepare the terminal

    Set-Location C:\Users\sharidass\Downloads\lakehouse-base-build
    docker compose --env-file .env --profile all ps
    .\check_all.ps1

### Say

> “I am separating infrastructure health from data correctness. A container can be running while a DAG, quality check, notebook, or vector index is failing. I will validate the important transitions with actual operations.”

If Airflow shows Restarting:

> “The Airflow webserver is available, but the scheduler needs dependency remediation. I will show the orchestration design and run the data path manually so the data results remain reproducible.”

---

## 2. Opening and architecture — 5 minutes

### Say

> “This is an enterprise manufacturing lakehouse. We simulate MES, IQMS, SAP, TrackWise, TMS, and historian-style sources. Batch extracts and streaming events converge into one open lakehouse.”

> “SeaweedFS is the S3-compatible object store. Iceberg provides table semantics, snapshots, schema management, and time travel. Spark handles ingestion and Kafka-to-Bronze processing. dbt creates governed Silver and Gold models. Trino is the shared SQL layer. Superset and Jupyter consume Gold data. RAG is a separate document and natural-language SQL path.”

### Draw this

    CSV/files --------------------\
                                    -> NiFi/Kafka -> Spark -> Bronze Iceberg
    CDC/event producers -----------/                         |
                                                            v
                                                 dbt Silver -> dbt Gold
                                                            |
                                                            v
                                                          Trino
                                                 /----------|----------\
                                              Superset   Jupyter   RAG SQL tool

    Documents -> Tika -> Ollama embeddings -> Milvus -> RAG assistant

### Technical point

> “Bronze is replayable and source-shaped. Silver standardizes, types, deduplicates, and conforms data. Gold is the business-facing serving layer. Consumers normally use Gold rather than raw Bronze.”

---

## 3. Infrastructure and storage — 7 minutes

### Run

    docker ps --format "table {{.Names}} {{.Status}} {{.Ports}}"
    curl.exe -s http://localhost:9333/cluster/status
    curl.exe -s http://localhost:8180/v1/info
    curl.exe -s http://localhost:8500/health
    curl.exe -s http://localhost:9090/-/healthy
    curl.exe -s http://localhost:9091/healthz

### Say

> “SeaweedFS has master, volume, filer, S3, and proxy roles. Trino is the SQL endpoint over the Iceberg catalog. Superset is the BI endpoint. Prometheus, Grafana, and Loki are the operational layer.”

Open SeaweedFS and show Bronze, Silver, Gold, warehouse, Parquet files, and Iceberg metadata.

> “The Parquet files are not the table by themselves. Iceberg metadata describes snapshots and manifests. That metadata provides table-level behavior on object storage.”

### Run

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW CATALOGS"
    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW SCHEMAS FROM iceberg"
    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM iceberg.bronze"
    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM iceberg.silver"
    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM iceberg.gold"

---

## 4. Streaming sources and Kafka — 8 minutes

### Say

> “Streaming data can come from CDC or event producers. Kafka Connect captures database changes, while the synthetic generator represents operational event producers. Kafka decouples producers from consumers and retains records for replay.”

### Run

    curl.exe -s http://localhost:8083/connectors
    curl.exe -s http://localhost:8083/connectors/mes-postgres-connector/status
    curl.exe -s http://localhost:8083/connectors/iqms-postgres-connector/status
    curl.exe -s http://localhost:8083/connectors/tms-postgres-connector/status
    docker exec lakehouse_kafka kafka-topics --bootstrap-server localhost:9092 --list

### Show a topic offset

    docker exec lakehouse_kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mes.production_orders --time -1

### Say

> “The offset is the durable position in a Kafka partition. Spark can process only records after the previous offset. The Bronze projection also stores Kafka offset, source system, ingestion time, key, and row hash.”

### Prove fresh activity

    docker exec lakehouse_kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mes.production_orders --time -1
    Start-Sleep -Seconds 15
    docker exec lakehouse_kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mes.production_orders --time -1
    docker logs lakehouse_synthetic_datagen --tail 20

> “If the second high-water mark is larger, records arrived during the call. If it is unchanged, this run is showing the existing backlog rather than a fresh interval of streaming.”

---

## 5. NiFi ingestion layer — 10 minutes

### Say

> “NiFi is the visual ingestion and routing layer. It handles file pickup, format conversion, provenance, back pressure, routing, and delivery to multiple destinations.”

### Batch flow

Show the visible process group and explain:

    ListFile -> FetchFile -> SplitRecord -> ConvertRecord
                                      |-> PublishKafka
                                      |-> PutS3Object

> “A file is discovered, fetched, split into records, converted to JSON, published to Kafka, and archived to object storage. Kafka gives us the event stream; SeaweedFS gives us the raw archive.”

### Streaming flow

    ConsumeKafka -> EvaluateJsonPath -> RouteOnAttribute
                                          |-> PASS archive
                                          |-> FAIL/quarantine archive

> “This is content-based routing. A status, quality result, plant, product, or severity can determine the destination. NiFi provides a visual audit trail and back pressure.”

### Check the API

    curl.exe -s http://localhost:8090/nifi-api/flow/process-groups/root

### Optional live batch trigger

Only run this if the NiFi UI shows the expected watched directory and running processors:

    docker exec lakehouse_nifi sh -lc "cp /opt/nifi/data/source/iqms_orders.csv /opt/nifi/data/source/iqms_orders_demo_$(date +%H%M%S).csv"

Then show FlowFile counters, provenance, and SeaweedFS output. If the mount is not available, explain the designed flow without claiming live processing.

### Technical summary

- NiFi provenance: where did this record come from?
- Kafka retention: can the event be replayed?
- SeaweedFS archive: can the original payload be audited?
- Iceberg Bronze: can the data be queried as a table?
- dbt: how does it become a business metric?

---

## 6. Batch ingestion with Spark — 8 minutes

### Say

> “I will now run a controlled batch ingestion. This creates fresh demo orders, uploads the source file to SeaweedFS, and appends the records to Bronze Iceberg.”

### Run

    docker exec lakehouse_spark_master spark-submit /opt/lakehouse/scripts/demo_ingest.py

For shorter display:

    docker exec lakehouse_spark_master spark-submit /opt/lakehouse/scripts/demo_ingest.py 2>&1 | Select-String "STEP|Uploading|Ingested|DEMO|ERROR|complete"

### Prove Bronze

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT order_id, product_code, status, CAST(_ingested_at AS varchar) AS ingested_at FROM iceberg.bronze.iqms_orders WHERE order_id LIKE 'DEMO-%' ORDER BY _ingested_at DESC LIMIT 10"

### Say

> “The source object and the table append are both visible. This is a batch source, but it lands in the same Bronze layer as the streaming path. That convergence is the core lakehouse design.”

---

## 7. Streaming-to-Bronze processing — 8 minutes

### Run

    powershell -ExecutionPolicy Bypass -File .\scripts\run_streaming_demo.ps1

Watch for missing topics, records appended, before/after counts, and dbt exit status.

### Validate the tables

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT 'mes_production_orders' AS table_name, count(*) AS rows, CAST(max(_ingested_at) AS varchar) AS last_ingest FROM iceberg.bronze.mes_production_orders UNION ALL SELECT 'iqms_quality_tests', count(*), CAST(max(_ingested_at) AS varchar) FROM iceberg.bronze.iqms_quality_tests UNION ALL SELECT 'iqms_deviations', count(*), CAST(max(_ingested_at) AS varchar) FROM iceberg.bronze.iqms_deviations"

### Say

> “This job is a bounded Kafka read that appends records into Iceberg. A continuously running production version would use Structured Streaming and a durable checkpoint. The local job demonstrates the same Kafka projection and offset-aware Bronze pattern.”

If counts do not increase:

> “The tables contain the stream backlog, but this particular run did not consume a new interval. I am distinguishing the implemented ingestion path from a fresh append observed during this call.”

---

## 8. dbt Silver — 8 minutes

### Say

> “Bronze is raw and source-shaped. Silver is where we standardize names and types, deduplicate, apply business rules, and join related systems.”

### Run from the dedicated dbt container

    docker exec lakehouse_dbt dbt run --select silver --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
    docker exec lakehouse_dbt dbt test --select silver --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt

Use the actual dbt summary rather than quoting a fixed test count.

### Show Silver rows

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT order_id, product_code, status, round(yield, 2) AS yield_pct, production_efficiency, round(cost_per_unit, 2) AS cost_per_unit FROM iceberg.silver.silver_production_orders WHERE order_id LIKE 'DEMO-%' ORDER BY order_id DESC LIMIT 10"

### Say

> “The raw record now has typed and derived fields. This is a reusable analytical contract, expressed as version-controlled, testable dbt SQL.”

---

## 9. dbt Gold and business metrics — 8 minutes

### Run

    docker exec lakehouse_dbt dbt run --select gold --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
    docker exec lakehouse_dbt dbt test --select gold --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt

### Batch release decisions

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT batch_id, product_code, round(yield_pct, 2) AS yield_pct, total_deviations, critical_deviations, batch_status FROM iceberg.gold.gold_batch_summary ORDER BY critical_deviations DESC LIMIT 10"

### Quality KPIs

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT product_code, total_batches, right_first_time_pct, avg_yield_pct, total_deviations, critical_deviation_rate FROM iceberg.gold.gold_quality_kpis ORDER BY right_first_time_pct DESC"

### Say

> “Gold is a business-facing model. It turns source-specific records into batch release, quality, inventory, training, CAPA, and manufacturing KPI views. BI users do not need to understand the source-system details.”

---

## 10. Data quality — 5 minutes

Explain the dbt tests:

- Not-null constraints.
- Unique business keys.
- Accepted status values.
- Relationships between Silver models.
- Gold mart uniqueness and required dimensions.

Run the separate GX check only if you want to show the known issue:

    docker exec lakehouse_airflow_scheduler python /opt/airflow/scripts/run_gx_validation.py

If the Great Expectations import error appears:

> “dbt validation is passing, but the Great Expectations runtime has a package API mismatch. This is a validation-tool dependency issue and needs a pinned compatible version.”

Do not call that GX run successful.

---

## 11. Trino and Iceberg — 6 minutes

### Run

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW SCHEMAS FROM iceberg"
    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM iceberg.gold"

### Say

> “Trino is the shared serving layer. Superset, Jupyter, and the SQL capability in the assistant can query the same Gold tables through one SQL interface.”

### Inspect snapshots if available

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT * FROM iceberg.gold.\"gold_batch_summary$snapshots\" ORDER BY committed_at DESC LIMIT 5"

If the metadata table name differs, use SHOW TABLES FROM iceberg.gold.

> “Snapshots support audit, reproducibility, time travel, and recovery scenarios.”

---

## 12. Superset — 7 minutes

### Bootstrap if required

    docker cp scripts\setup_superset.py lakehouse_superset:/tmp/setup_superset.py
    docker exec lakehouse_superset python /tmp/setup_superset.py

Open http://localhost:8500, log in, and open Manufacturing Lakehouse - Live Demo.

### Say

> “Superset is the BI presentation layer over Gold through Trino. It is not another analytical copy; the charts query the governed serving layer.”

### SQL Lab query

    SELECT product_code, batch_status,
           count(*) AS batch_count,
           round(avg(yield_pct), 2) AS avg_yield,
           sum(critical_deviations) AS total_critical
    FROM iceberg.gold.gold_batch_summary
    GROUP BY product_code, batch_status
    ORDER BY product_code, batch_status;

Refresh after dbt and explain that the dashboard is reading the current Gold tables.

---

## 13. JupyterLab — 7 minutes

Open http://localhost:8400 and open lakehouse_demo.ipynb.

Recommended sequence:

1. Connect to Trino.
2. Count Bronze/Silver/Gold tables.
3. Plot data volume by layer.
4. Show recent streaming rows.
5. Plot Silver production or quality metrics.
6. Plot a Gold KPI.

### Verify the OEE schema before presenting that cell

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "DESCRIBE iceberg.gold.gold_oee_dashboard"

The current live schema includes:

    machine_id, hour_window, product_code, total_events,
    pass_count, fail_count, quality_rate, avg_temp,
    avg_pressure, window_start, window_end

Use a compatible query:

    SELECT product_code,
           round(avg(quality_rate), 3) AS quality_rate,
           sum(total_events) AS total_events,
           sum(pass_count) AS pass_count,
           sum(fail_count) AS fail_count
    FROM iceberg.gold.gold_oee_dashboard
    GROUP BY product_code
    ORDER BY quality_rate DESC;

### Say

> “Jupyter is the exploration surface. It uses the same governed Trino layer as Superset, but it gives data scientists a reproducible environment for Python, pandas, and visual analysis.”

---

## 14. Airflow — 5 minutes

Open http://localhost:8280.

### Say

> “Airflow is intended to coordinate source checks, NiFi deployment, Spark ingestion, Kafka ingestion, dbt Silver, validation, dbt Gold, and lineage emission. Gold should not run when upstream validation fails.”

### Check the scheduler

    docker ps -a --format "{{.Names}}|{{.Status}}" | Select-String "airflow_scheduler|airflow_web"
    docker logs lakehouse_airflow_scheduler --tail 40

If the scheduler shows the RLock/Pydantic failure:

> “The DAG design is present, but the scheduler runtime needs dependency pinning and an image rebuild. I am demonstrating the dependency order manually today.”

Do not trigger a DAG and call it successful while the scheduler is restarting.

---

## 15. RAG and natural-language SQL — 8 minutes

### Explain the document path

    Documents -> SeaweedFS -> Tika -> chunks
              -> Ollama embeddings -> Milvus
              -> LangChain retrieval -> Ollama answer

The SQL path is separate:

    Question -> Ollama SQL generation
             -> SELECT-only validation
             -> Trino Gold query

### Check services

    curl.exe -s http://localhost:11434/api/tags
    curl.exe -s http://localhost:9091/healthz
    curl.exe -s http://localhost:8501

### Check the Milvus collection

    docker exec lakehouse_langchain_app python -c "from pymilvus import connections,utility,Collection; connections.connect(host='milvus',port=19530); print(utility.list_collections()); print(Collection('enterprise_documents').num_entities if utility.has_collection('enterprise_documents') else 0)"

### Index only before the call

    docker compose --env-file .env --profile all run --rm --no-deps langchain-indexer

This can take a long time because embeddings run locally.

### Say

> “The assistant has two modes: document RAG, which retrieves source chunks from Milvus, and SQL analytics, which generates a read-only query over Gold through Trino.”

If indexing is incomplete:

> “The AI services are running, but the index is not fully validated for this demonstration. The completed analytics path is the authoritative end-to-end result.”

---

## 16. Monitoring and governance — 6 minutes

### Run

    curl.exe -s http://localhost:3000/api/health
    curl.exe -s http://localhost:9090/api/v1/targets
    curl.exe -s http://localhost:3100/ready
    curl.exe -s http://localhost:8880/health

Open Grafana and show pipeline health, node/container metrics, and logs.

Open DataHub and show dataset search, descriptions, ownership, and lineage if populated.

### Say

> “Monitoring is the control plane for health and performance. Governance is the control plane for meaning, ownership, discovery, and lineage. Neither replaces the data path; they make the data path operable.”

---

## 17. Final proof — 5 minutes

### Final layer counts

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT 'bronze.iqms_orders' AS table_name, count(*) AS rows FROM iceberg.bronze.iqms_orders UNION ALL SELECT 'bronze.mes_events', count(*) FROM iceberg.bronze.mes_events UNION ALL SELECT 'silver.silver_production_orders', count(*) FROM iceberg.silver.silver_production_orders UNION ALL SELECT 'silver.silver_quality_events', count(*) FROM iceberg.silver.silver_quality_events UNION ALL SELECT 'gold.gold_batch_summary', count(*) FROM iceberg.gold.gold_batch_summary UNION ALL SELECT 'gold.gold_quality_kpis', count(*) FROM iceberg.gold.gold_quality_kpis ORDER BY table_name"

### Final business question

    docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SELECT batch_status, count(*) AS batches, round(avg(yield_pct), 2) AS avg_yield, sum(critical_deviations) AS critical_deviations FROM iceberg.gold.gold_batch_summary GROUP BY batch_status ORDER BY batch_status"

### Say

> “We started with operational files and events, landed them into open object storage and Iceberg tables, transformed them with governed SQL, validated the results, exposed them through Trino, and consumed them in BI and notebooks. The same platform also provides ingestion routing, monitoring, governance, and an AI path for documents and analytical questions.”

---

## 18. Q&A answers

**Why both Spark and dbt?**

> “Spark is suited to distributed ingestion and event processing. dbt is suited to governed, testable, version-controlled SQL business transformations.”

**Why not query Bronze directly?**

> “Bronze is source-shaped and useful for replay and audit. Silver and Gold provide stable analytical contracts.”

**What happens to bad data?**

> “NiFi can route or quarantine it, Bronze preserves the raw payload, dbt tests can stop downstream publication, and monitoring surfaces the failure.”

**How does streaming scale?**

> “Kafka partitions provide parallelism, Spark distributes processing, and Iceberg commits data files and metadata. The local demo is small, but the interfaces are designed for larger deployments.”

**What still needs hardening?**

> “Airflow dependency pinning, Great Expectations compatibility, a continuously running streaming job, notebook/schema alignment, and completed RAG indexing.”

---

## Timing guide

| Section | Time |
|---|---:|
| Opening and architecture | 5 min |
| Infrastructure and storage | 7 min |
| CDC/Kafka | 8 min |
| NiFi | 10 min |
| Batch Spark ingestion | 8 min |
| Streaming-to-Bronze | 8 min |
| dbt Silver | 8 min |
| dbt Gold | 8 min |
| Data quality | 5 min |
| Trino | 6 min |
| Superset | 7 min |
| JupyterLab | 7 min |
| Airflow | 5 min |
| RAG/SQL assistant | 8 min |
| Monitoring/governance | 6 min |
| Final proof/Q&A | 10 min |
| **Maximum** | **~116 min** |

For a 60–75 minute call, shorten NiFi, Airflow, and governance. For a 90–120 minute technical call, run every query and allow the UI walkthroughs to breathe.

