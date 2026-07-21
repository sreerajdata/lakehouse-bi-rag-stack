#!/bin/sh
# init-openbao.sh
# Populates the OpenBao (Vault-compatible) dev-mode instance with
# lakehouse platform credentials at stack startup.
# Runs as a one-shot container (openbao-init) after openbao is started.
#
# Usage: mounted into openbao-init container at /scripts/init-openbao.sh
# BAO_ADDR and BAO_TOKEN are injected via docker-compose environment.

set -e

BAO_ADDR="${BAO_ADDR:-http://openbao:8200}"
BAO_TOKEN="${BAO_TOKEN:-roottoken}"

echo "[OpenBao Init] Waiting for OpenBao to be ready at ${BAO_ADDR}..."
RETRIES=30
until wget -q --spider "${BAO_ADDR}/v1/sys/health" 2>/dev/null; do
  RETRIES=$((RETRIES - 1))
  if [ "$RETRIES" -le 0 ]; then
    echo "[OpenBao Init] ERROR: OpenBao did not become ready in time."
    exit 1
  fi
  sleep 2
done
echo "[OpenBao Init] OpenBao is ready."

export VAULT_ADDR="${BAO_ADDR}"
export VAULT_TOKEN="${BAO_TOKEN}"

# ── Enable KV v2 secrets engine at secret/ (dev mode has it at secret/ by default,
#    but we enable it explicitly so this script is safe to re-run) ──────────────
bao secrets enable -path=secret kv-v2 2>/dev/null || echo "[OpenBao Init] KV engine already enabled at secret/"

# ── Core Platform Credentials ─────────────────────────────────────────────────
# These mirror the current .env values. In a production setup, rotate these
# and remove them from .env, sourcing only from Vault.

bao kv put secret/lakehouse/postgres \
  username="admin" \
  password="admin123" \
  host="lakehouse_postgres" \
  port="5432" \
  airflow_db="airflow" \
  hive_metastore_db="hive_metastore" \
  superset_db="superset"

echo "[OpenBao Init] secret/lakehouse/postgres populated."

bao kv put secret/lakehouse/seaweedfs \
  access_key="admin" \
  secret_key="admin123" \
  endpoint="http://seaweedfs-s3:8333" \
  bronze_bucket="lakehouse-bronze" \
  silver_bucket="lakehouse-silver" \
  gold_bucket="lakehouse-gold" \
  models_bucket="lakehouse-models" \
  docs_bucket="lakehouse-docs"

echo "[OpenBao Init] secret/lakehouse/seaweedfs populated."

bao kv put secret/lakehouse/kafka \
  bootstrap_servers="kafka:9092" \
  schema_registry_url="http://schema-registry:8081"

echo "[OpenBao Init] secret/lakehouse/kafka populated."

bao kv put secret/lakehouse/nifi \
  username="admin" \
  password="adminadminadmin" \
  api_url="http://nifi:8090/nifi-api"

echo "[OpenBao Init] secret/lakehouse/nifi populated."

bao kv put secret/lakehouse/trino \
  host="trino" \
  port="8080" \
  catalog="iceberg" \
  user="admin"

echo "[OpenBao Init] secret/lakehouse/trino populated."

bao kv put secret/lakehouse/airflow \
  fernet_key="81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=" \
  admin_password="admin"

echo "[OpenBao Init] secret/lakehouse/airflow populated."

bao kv put secret/lakehouse/superset \
  secret_key="supersecret123"

echo "[OpenBao Init] secret/lakehouse/superset populated."

bao kv put secret/lakehouse/grafana \
  admin_user="admin" \
  admin_password="admin123"

echo "[OpenBao Init] secret/lakehouse/grafana populated."

bao kv put secret/lakehouse/opensearch \
  initial_admin_password="Admin@123456"

echo "[OpenBao Init] secret/lakehouse/opensearch populated."

bao kv put secret/lakehouse/datahub_mysql \
  root_password="datahub" \
  user="datahub" \
  password="datahub"

echo "[OpenBao Init] secret/lakehouse/datahub_mysql populated."

# ── Verify all secrets are readable ──────────────────────────────────────────
echo ""
echo "[OpenBao Init] ══════════════════════════════════════════════"
echo "[OpenBao Init] Verifying populated secrets:"
for path in \
  secret/lakehouse/postgres \
  secret/lakehouse/seaweedfs \
  secret/lakehouse/kafka \
  secret/lakehouse/nifi \
  secret/lakehouse/trino \
  secret/lakehouse/airflow \
  secret/lakehouse/superset \
  secret/lakehouse/grafana \
  secret/lakehouse/opensearch \
  secret/lakehouse/datahub_mysql; do
    if bao kv get "$path" > /dev/null 2>&1; then
        echo "[OpenBao Init]   ✅  $path"
    else
        echo "[OpenBao Init]   ❌  $path  (MISSING)"
    fi
done

echo "[OpenBao Init] ══════════════════════════════════════════════"
echo "[OpenBao Init] All secrets loaded. OpenBao is the source of truth."
echo "[OpenBao Init] Access secrets via: bao kv get secret/lakehouse/<service>"
echo "[OpenBao Init] Web UI: http://localhost:8200  Token: ${BAO_TOKEN}"
