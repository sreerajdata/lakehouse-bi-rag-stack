#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TOTAL=0
HEALTHY=0
FAILED=0

FAILED_SERVICES=()
FAILED_REASONS=()
FAILED_LOG_TARGETS=()

POSTGRES_USER="${POSTGRES_USER:-postgres}"

check_http_contains() {
  local url="$1"
  local expected="${2:-}"
  local body

  body="$(curl -fsS --max-time 10 "$url" 2>&1)" || return 1
  if [[ -n "$expected" ]] && [[ "$body" != *"$expected"* ]]; then
    echo "unexpected response"
    return 2
  fi
  return 0
}

check_cmd() {
  "$@" >/tmp/lakehouse_healthcheck_cmd.out 2>/tmp/lakehouse_healthcheck_cmd.err
}

record_result() {
  local service="$1"
  local success="$2"
  local reason="${3:-}"
  local log_target="${4:-}"

  TOTAL=$((TOTAL + 1))
  if [[ "$success" == "true" ]]; then
    HEALTHY=$((HEALTHY + 1))
    printf '✅ %s — OK\n' "$service"
  else
    FAILED=$((FAILED + 1))
    printf '❌ %s — FAILED (%s)\n' "$service" "$reason"
    FAILED_SERVICES+=("$service")
    FAILED_REASONS+=("$reason")
    FAILED_LOG_TARGETS+=("$log_target")
  fi
}

run_http_check() {
  local service="$1"
  local url="$2"
  local expected="${3:-}"
  local log_target="${4:-}"
  local body

  if body="$(curl -fsS --max-time 10 "$url" 2>&1)"; then
    if [[ -n "$expected" ]] && [[ "$body" != *"$expected"* ]]; then
      record_result "$service" false "unexpected response from $url" "$log_target"
    else
      record_result "$service" true
    fi
  else
    record_result "$service" false "$body" "$log_target"
  fi
}

run_http_status_check() {
  local service="$1"
  local url="$2"
  local allowed_csv="$3"
  local log_target="${4:-}"
  local http_code

  http_code="$(curl -sS -o /tmp/lakehouse_healthcheck_http.out -w "%{http_code}" --max-time 10 "$url" 2>/tmp/lakehouse_healthcheck_http.err)" || {
    local err
    err="$(< /tmp/lakehouse_healthcheck_http.err)"
    record_result "$service" false "$err" "$log_target"
    return
  }

  if [[ ",$allowed_csv," == *",$http_code,"* ]]; then
    record_result "$service" true
  else
    record_result "$service" false "unexpected HTTP $http_code from $url" "$log_target"
  fi
}

run_exec_check() {
  local service="$1"
  local log_target="$2"
  shift 2
  local output

  if output="$("$@" 2>&1)"; then
    record_result "$service" true
  else
    output="${output//$'\n'/ ; }"
    record_result "$service" false "$output" "$log_target"
  fi
}

show_failure_logs() {
  local service="$1"
  local log_target="$2"

  [[ -z "$log_target" ]] && return 0

  printf '\n----- Last 50 log lines for %s (%s) -----\n' "$service" "$log_target"
  if docker logs --tail 50 "$log_target" 2>/dev/null; then
    return 0
  fi

  docker compose logs --tail 50 "$log_target" 2>/dev/null || true
}

run_http_check "SeaweedFS Master" "http://localhost:9333/cluster/status" "\"IsLeader\"" "lakehouse_seaweedfs_master"
run_http_status_check "SeaweedFS S3" "http://localhost:8333" "200,403" "lakehouse_seaweedfs_s3"
run_exec_check "PostgreSQL" "lakehouse_postgres" docker exec lakehouse_postgres pg_isready -U "$POSTGRES_USER"
run_exec_check "Redis" "lakehouse_redis" docker exec lakehouse_redis redis-cli ping
run_exec_check "Zookeeper" "lakehouse_zookeeper" docker exec lakehouse_zookeeper bash -lc "printf 'srvr\n' | nc -w 5 localhost 2181 | grep -q Mode"
run_exec_check "Kafka" "lakehouse_kafka" docker exec lakehouse_kafka kafka-broker-api-versions --bootstrap-server localhost:9092
run_http_check "Kafka Connect" "http://localhost:8083/connectors" "" "lakehouse_kafka_connect"
run_http_check "Schema Registry" "http://localhost:8081/subjects" "" "lakehouse_schema_registry"
run_http_check "Apache NiFi" "http://localhost:8090/nifi-api/system-diagnostics" "\"systemDiagnostics\"" "lakehouse_nifi"
run_exec_check "Hive Metastore" "lakehouse_hive_metastore" docker exec lakehouse_hive_metastore bash -lc "cat < /dev/null > /dev/tcp/localhost/9083"
run_http_check "Trino" "http://localhost:8080/v1/info" "\"nodeVersion\"" "lakehouse_trino"
run_http_check "Spark Master" "http://localhost:8998" "" "lakehouse_spark_master"
run_http_status_check "Apache Tika" "http://localhost:9998/tika" "200,405" "lakehouse_tika"
run_http_check "Airflow" "http://localhost:8793/health" "\"metadatabase\"" "lakehouse_airflow_web"
run_http_check "Apache Superset" "http://localhost:8088/health" "OK" "lakehouse_superset"
run_http_check "OpenSearch" "http://localhost:9200/_cluster/health" "\"status\"" "lakehouse_opensearch"
run_http_check "Grafana" "http://localhost:3000/api/health" "\"database\"" "lakehouse_grafana"
run_http_check "Prometheus" "http://localhost:9090/-/ready" "Ready" "lakehouse_prometheus"

printf '\nSummary: %s/%s services healthy\n' "$HEALTHY" "$TOTAL"

if (( FAILED > 0 )); then
  for i in "${!FAILED_SERVICES[@]}"; do
    show_failure_logs "${FAILED_SERVICES[$i]}" "${FAILED_LOG_TARGETS[$i]}"
  done
  exit 1
fi

exit 0
