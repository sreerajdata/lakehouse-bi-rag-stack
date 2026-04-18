# ============================================================
# TPL Data Lakehouse - Stack Management Makefile
# ============================================================

.PHONY: help up-core up-ingestion up-processing up-lakehouse \
        up-analytics up-ai up-monitoring up-governance up-cicd \
        up-all down clean logs status pull-images \
        register-connectors integration-test dbt-compile

COMPOSE = docker compose
ENV_FILE = --env-file .env

help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════╗"
	@echo "║       TPL Data Lakehouse - Build Commands               ║"
	@echo "╚══════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  Incremental startup (recommended order):"
	@echo "  ─────────────────────────────────────────"
	@echo "  make up-core          → SeaweedFS + PostgreSQL"
	@echo "  make up-ingestion     → Kafka + NiFi + Debezium"
	@echo "  make up-processing    → Spark + Airflow + dbt"
	@echo "  make up-lakehouse     → Hive Metastore + Trino"
	@echo "  make up-analytics     → JupyterHub + Superset"
	@echo "  make up-ai            → Ollama + Milvus + Streamlit"
	@echo "  make up-monitoring    → Prometheus + Grafana + Loki"
	@echo "  make up-governance    → DataHub + OpenSearch + OpenBao"
	@echo "  make up-cicd          → GitLab CE"
	@echo "  make up-synthetic     → Synthetic data generator"
	@echo ""
	@echo "  Full stack:"
	@echo "  ─────────────────────────────────────────"
	@echo "  make up-all           → Start complete stack"
	@echo "  make down             → Stop all services"
	@echo "  make clean            → Stop + remove volumes"
	@echo "  make status           → Show running services"
	@echo "  make logs svc=<name>  → Tail service logs"
	@echo "  make pull-images      → Pre-pull all images (airgap prep)"
	@echo "  make save-images      → Save images to tar (airgap transfer)"
	@echo ""
	@echo "  Service UIs:"
	@echo "  ─────────────────────────────────────────"
	@echo "  Airflow:     http://localhost:8280  (admin/admin)"
	@echo "  Spark:       http://localhost:8181"
	@echo "  Trino:       http://localhost:8180"
	@echo "  Kafka UI:    http://localhost:9000"
	@echo "  NiFi:        http://localhost:8090"
	@echo "  Superset:    http://localhost:8500  (admin/admin)"
	@echo "  JupyterHub:  http://localhost:8400"
	@echo "  Grafana:     http://localhost:3000  (admin/admin123)"
	@echo "  Prometheus:  http://localhost:9090"
	@echo "  DataHub:     http://localhost:9002"
	@echo "  OpenBao:     http://localhost:8200"
	@echo "  Ollama/Chat: http://localhost:8501"
	@echo "  Admin:       http://localhost:8502  (Ops Dashboard)"
	@echo "  GitLab:      http://localhost:8929"
	@echo "  SeaweedFS:   http://localhost:8333  (S3 endpoint)"
	@echo ""

# ── Start by profile ──────────────────────────────────────────────────────────
up-core:
	@echo "🚀 Starting core infrastructure (SeaweedFS + PostgreSQL)..."
	$(COMPOSE) $(ENV_FILE) --profile core up -d
	@echo "⏳ Waiting for PostgreSQL..."
	@sleep 5
	@echo "✅ Core infrastructure running"

up-ingestion: up-core
	@echo "🚀 Starting data ingestion layer..."
	$(COMPOSE) $(ENV_FILE) --profile ingestion up -d
	@echo "✅ Ingestion layer running"

up-processing: up-core
	@echo "🚀 Starting processing layer (Spark + Airflow)..."
	$(COMPOSE) $(ENV_FILE) --profile processing up -d
	@echo "✅ Processing layer running"

up-lakehouse: up-core
	@echo "🚀 Starting lakehouse layer (Hive + Trino + Iceberg)..."
	$(COMPOSE) $(ENV_FILE) --profile lakehouse up -d
	@echo "✅ Lakehouse layer running"

up-analytics: up-lakehouse
	@echo "🚀 Starting analytics layer (JupyterHub + Superset)..."
	$(COMPOSE) $(ENV_FILE) --profile analytics up -d
	@echo "✅ Analytics layer running"

up-ai: up-lakehouse
	@echo "🚀 Starting AI layer (Ollama + Milvus + LangChain)..."
	$(COMPOSE) $(ENV_FILE) --profile ai up -d
	@echo "✅ AI layer running"

up-monitoring:
	@echo "🚀 Starting monitoring layer (Prometheus + Grafana + Loki)..."
	$(COMPOSE) $(ENV_FILE) --profile monitoring up -d
	@echo "✅ Monitoring layer running"

up-governance: up-core
	@echo "🚀 Starting governance layer (DataHub + OpenSearch + OpenBao)..."
	$(COMPOSE) $(ENV_FILE) --profile governance up -d
	@echo "✅ Governance layer running"

up-cicd:
	@echo "🚀 Starting GitLab CE (allow 5-10 min to initialize)..."
	$(COMPOSE) $(ENV_FILE) --profile cicd up -d

up-synthetic: up-ingestion
	@echo "🚀 Starting synthetic data generator..."
	$(COMPOSE) $(ENV_FILE) --profile synthetic up -d
	@echo "✅ Synthetic data streaming to Kafka"

up-data-stack: up-core up-ingestion up-processing up-lakehouse up-analytics up-monitoring
	@echo "✅ Full data stack running (excluding AI, Governance, CI/CD)"

up-all:
	@echo "🚀 Starting complete TPL Data Lakehouse stack..."
	$(COMPOSE) $(ENV_FILE) --profile all up -d
	@echo ""
	@echo "✅ Full stack started. Services are initializing."
	@echo "   Run 'make status' to see service health."

# ── Management ────────────────────────────────────────────────────────────────
down:
	$(COMPOSE) $(ENV_FILE) --profile all down

clean:
	$(COMPOSE) $(ENV_FILE) --profile all down -v --remove-orphans
	@echo "⚠️  All volumes removed. Data is gone."

status:
	$(COMPOSE) $(ENV_FILE) --profile all ps

logs:
	$(COMPOSE) $(ENV_FILE) --profile all logs -f $(svc)

restart:
	$(COMPOSE) $(ENV_FILE) --profile all restart $(svc)

# ── Airgap Image Management ───────────────────────────────────────────────────
IMAGES = \
  postgres:15 \
  chrislusf/seaweedfs:3.63 \
  amazon/aws-cli:2.15.0 \
  confluentinc/cp-zookeeper:7.5.0 \
  confluentinc/cp-kafka:7.5.0 \
  confluentinc/cp-schema-registry:7.5.0 \
  confluentinc/cp-kafka-connect:7.5.0 \
  provectuslabs/kafka-ui:latest \
  apache/nifi:1.23.2 \
  bitnami/spark:3.4 \
  apache/airflow:2.8.1 \
  redis:7-alpine \
  ghcr.io/dbt-labs/dbt-trino:1.7.2 \
  apache/tika:2.9.1 \
  apache/hive:4.0.0-alpha-2 \
  trinodb/trino:435 \
  jupyterhub/jupyterhub:4.0 \
  apache/superset:3.1.0 \
  ollama/ollama:latest \
  quay.io/coreos/etcd:v3.5.5 \
  milvusdb/milvus:v2.4.0 \
  prom/prometheus:v2.48.0 \
  grafana/grafana:10.2.0 \
  grafana/loki:2.9.0 \
  grafana/promtail:2.9.0 \
  opensearchproject/opensearch:2.11.0 \
  mysql:8.0 \
  acryldata/datahub-gms:head \
  acryldata/datahub-frontend-react:head \
  neo4j:4.4.9 \
  openbao/openbao:latest \
  gitlab/gitlab-ce:16.7.0-ce.0 \
  hertzg/tesseract-server:latest \
  prom/node-exporter:v1.7.0 \
  prometheuscommunity/postgres-exporter:v0.15.0

pull-images:
	@echo "Pulling all images for airgap preparation..."
	@for img in $(IMAGES); do \
		echo "Pulling $$img..."; \
		docker pull $$img; \
	done
	@echo "✅ All images pulled."

save-images:
	@mkdir -p ./airgap-images
	@echo "Saving images to ./airgap-images/ ..."
	@for img in $(IMAGES); do \
		filename=$$(echo $$img | tr '/:' '__'); \
		echo "Saving $$img → $$filename.tar"; \
		docker save $$img | gzip > ./airgap-images/$$filename.tar.gz; \
	done
	@echo "✅ All images saved."

load-images:
	@echo "Loading images from ./airgap-images/ ..."
	@for f in ./airgap-images/*.tar.gz; do \
		echo "Loading $$f..."; \
		docker load < $$f; \
	done
	@echo "✅ All images loaded."

# ── Utilities ─────────────────────────────────────────────────────────────────
init-buckets:
	@$(COMPOSE) $(ENV_FILE) --profile core run --rm seaweedfs-init

trino-shell:
	@docker exec -it lakehouse_trino trino --server http://localhost:8080 --catalog iceberg

spark-shell:
	@docker exec -it lakehouse_spark_master spark-shell \
		--master spark://spark-master:7077 \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

kafka-topics:
	@docker exec lakehouse_kafka kafka-topics --bootstrap-server localhost:9092 --list

dbt-run:
	@docker exec lakehouse_dbt dbt run --profiles-dir /usr/app/dbt

dbt-test:
	@docker exec lakehouse_dbt dbt test --profiles-dir /usr/app/dbt

ollama-pull-llama3:
	@docker exec lakehouse_ollama ollama pull llama3

health-check:
	@echo "Checking service health..."
	@curl -sf http://localhost:9333/cluster/status && echo "✅ SeaweedFS" || echo "❌ SeaweedFS"
	@curl -sf http://localhost:8280/health && echo "✅ Airflow" || echo "❌ Airflow"
	@curl -sf http://localhost:8180/v1/info && echo "✅ Trino" || echo "❌ Trino"
	@curl -sf http://localhost:9090/-/healthy && echo "✅ Prometheus" || echo "❌ Prometheus"
	@curl -sf http://localhost:3000/api/health && echo "✅ Grafana" || echo "❌ Grafana"
	@curl -sf http://localhost:9000 && echo "✅ Kafka UI" || echo "❌ Kafka UI"
	@curl -sf http://localhost:8501 && echo "✅ AI Chat" || echo "❌ AI Chat"
	@curl -sf http://localhost:8502 && echo "✅ Admin Dashboard" || echo "❌ Admin Dashboard"

# ── CDC & Data Quality ────────────────────────────────────────────────────────
register-connectors:
	@echo "📡 Registering Kafka Connect CDC connectors..."
	@bash scripts/register_connectors.sh

dbt-compile:
	@docker exec lakehouse_dbt dbt compile --profiles-dir /usr/app/dbt

# ── Integration Testing ──────────────────────────────────────────────────────
integration-test:
	@echo "🧪 Running integration tests..."
	@python scripts/integration_test.py
