#!/bin/bash
echo "=== CORE INFRASTRUCTURE ===" 

echo "--- SeaweedFS Master ---"
curl -s http://localhost:9333/cluster/status

echo "--- SeaweedFS S3 ---"
curl -s http://localhost:8333/

echo "--- PostgreSQL ---"
docker exec lakehouse_postgres pg_isready -U admin

echo "=== INGESTION LAYER ==="

echo "--- Kafka ---"
docker exec lakehouse_kafka \
  kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1 | head -3

echo "--- Kafka UI ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:9000

echo "--- Schema Registry ---"
curl -s http://localhost:8081/subjects

echo "--- Kafka Connect ---"
curl -s http://localhost:8083/connectors

echo "--- NiFi ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8090/nifi

echo "=== PROCESSING LAYER ==="

echo "--- Spark Master ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8181

echo "--- Airflow ---"
curl -s http://localhost:8280/health

echo "--- Tika ---"
curl -s http://localhost:9998/tika

echo "=== LAKEHOUSE LAYER ==="

echo "--- Hive Metastore (port check) ---"
docker exec lakehouse_hive_metastore \
  bash -c "cat < /dev/null > /dev/tcp/localhost/9083" \
  && echo "Hive port 9083 OPEN" || echo "Hive port 9083 CLOSED"

echo "--- Trino ---"
curl -s http://localhost:8180/v1/info

echo "--- Trino Catalogs ---"
docker exec lakehouse_trino trino \
  --server http://localhost:8080 \
  --execute "SHOW CATALOGS;" 2>/dev/null

echo "=== ANALYTICS LAYER ==="

echo "--- Superset ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8500/health

echo "--- JupyterHub ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8400

echo "--- Admin Dashboard ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8502

echo "=== AI LAYER ==="

echo "--- Ollama ---"
curl -s http://localhost:11434/api/tags

echo "--- Milvus ---"
curl -s http://localhost:9091/healthz

echo "--- AI Chatbot (Streamlit) ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8501

echo "=== MONITORING LAYER ==="

echo "--- Prometheus ---"
curl -s http://localhost:9090/-/healthy

echo "--- Grafana ---"
curl -s http://localhost:3000/api/health

echo "--- Loki ---"
curl -s http://localhost:3100/ready

echo "--- Node Exporter ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:9100/metrics

echo "--- Postgres Exporter ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:9187/metrics

echo "=== GOVERNANCE LAYER ==="

echo "--- OpenSearch ---"
curl -s http://localhost:9200/_cluster/health

echo "--- DataHub Frontend ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:9002

echo "--- DataHub GMS ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8880/health

echo "--- OpenBao (Secrets) ---"
curl -s http://localhost:8200/v1/sys/health

echo "=== CI/CD ==="

echo "--- GitLab ---"
curl -so /dev/null -w "HTTP %{http_code}\n" http://localhost:8929

echo "=== DOCKER CONTAINER STATUS (all) ==="
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
