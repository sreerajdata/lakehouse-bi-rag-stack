Write-Output "=== CORE INFRASTRUCTURE ===" 
Write-Output "--- SeaweedFS Master ---"
curl.exe -s http://localhost:9333/cluster/status
Write-Output "--- SeaweedFS S3 ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8333/

Write-Output "--- PostgreSQL ---"
docker exec lakehouse_postgres pg_isready -U admin

Write-Output "=== INGESTION LAYER ==="
Write-Output "--- Kafka ---"
docker exec lakehouse_kafka kafka-broker-api-versions --bootstrap-server localhost:9092 | Select-Object -First 3

Write-Output "--- Kafka UI ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:9000

Write-Output "--- Schema Registry ---"
curl.exe -s http://localhost:8081/subjects

Write-Output "--- Kafka Connect ---"
curl.exe -s http://localhost:8083/connectors

Write-Output "--- NiFi ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8090/nifi

Write-Output "=== PROCESSING LAYER ==="
Write-Output "--- Spark Master ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8181

Write-Output "--- Airflow ---"
curl.exe -s http://localhost:8280/health

Write-Output "--- Tika ---"
curl.exe -s http://localhost:9998/tika

Write-Output "=== LAKEHOUSE LAYER ==="
Write-Output "--- Hive Metastore (port check) ---"
$hivePortOpen = Test-NetConnection -ComputerName localhost -Port 9083 -WarningAction SilentlyContinue
if ($hivePortOpen.TcpTestSucceeded) { Write-Output "Hive port 9083 OPEN" } else { Write-Output "Hive port 9083 CLOSED" }

Write-Output "--- Trino ---"
curl.exe -s http://localhost:8180/v1/info

Write-Output "--- Trino Catalogs ---"
docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW CATALOGS;" 2>$null

Write-Output "=== ANALYTICS LAYER ==="
Write-Output "--- Superset ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8500/health

Write-Output "--- JupyterHub ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8400

Write-Output "--- Admin Dashboard ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8502

Write-Output "=== AI LAYER ==="
Write-Output "--- Ollama ---"
curl.exe -s http://localhost:11434/api/tags

Write-Output "--- Milvus ---"
curl.exe -s http://localhost:9091/healthz

Write-Output "--- AI Chatbot (Streamlit) ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8501

Write-Output "=== MONITORING LAYER ==="
Write-Output "--- Prometheus ---"
curl.exe -s http://localhost:9090/-/healthy

Write-Output "--- Grafana ---"
curl.exe -s http://localhost:3000/api/health

Write-Output "--- Loki ---"
curl.exe -s http://localhost:3100/ready

Write-Output "--- Node Exporter ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:9100/metrics

Write-Output "--- Postgres Exporter ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:9187/metrics

Write-Output "=== GOVERNANCE LAYER ==="
Write-Output "--- OpenSearch ---"
curl.exe -s http://localhost:9200/_cluster/health

Write-Output "--- DataHub Frontend ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:9002

Write-Output "--- DataHub GMS ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8880/health

Write-Output "--- OpenBao (Secrets) ---"
curl.exe -s http://localhost:8200/v1/sys/health

Write-Output "=== CI/CD ==="
Write-Output "--- GitLab ---"
curl.exe -s -o NUL -w "HTTP %{http_code}\n" http://localhost:8929

Write-Output "=== DOCKER CONTAINER STATUS (all) ==="
docker compose --profile all ps --format "table {{.Name}}`t{{.Status}}"
