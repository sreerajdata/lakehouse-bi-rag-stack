#!/usr/bin/env bash

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
AWS_ACCESS_KEY_ID="${SEAWEEDFS_ACCESS_KEY:-admin}"
AWS_SECRET_ACCESS_KEY="${SEAWEEDFS_SECRET_KEY:-admin123}"

create_connector() {
  local name="$1"
  local topic="$2"
  local topics_dir="$3"

  local config_payload
  config_payload="$(cat <<JSON
{
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "1",
    "topics": "${topic}",
    "s3.bucket.name": "bronze",
    "s3.region": "us-east-1",
    "store.url": "http://seaweedfs-s3:8333",
    "s3.part.size": "5242880",
    "flush.size": "100",
    "rotate.schedule.interval.ms": "60000",
    "timezone": "UTC",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "topics.dir": "${topics_dir}",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    "schema.compatibility": "NONE",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter.schemas.enable": "false",
    "aws.access.key.id": "${AWS_ACCESS_KEY_ID}",
    "aws.secret.access.key": "${AWS_SECRET_ACCESS_KEY}"
  }
JSON
)"

  local payload
  payload="$(cat <<JSON
{
  "name": "${name}",
  "config": ${config_payload}
}
JSON
)"

  local http_code
  http_code="$(curl -s -o /tmp/${name}.out -w "%{http_code}" "${CONNECT_URL}/connectors/${name}")"
  if [[ "$http_code" == "200" ]]; then
    curl -sS -X PUT \
      -H "Content-Type: application/json" \
      --data "$config_payload" \
      "${CONNECT_URL}/connectors/${name}/config"
  else
    curl -sS -X POST \
      -H "Content-Type: application/json" \
      --data "$payload" \
      "${CONNECT_URL}/connectors"
  fi
  printf '\n'
}

create_connector "s3_sink_raw_mes_events" "raw.mes.events" "mes"
create_connector "s3_sink_raw_iqms_orders" "raw.iqms.orders" "iqms"
create_connector "s3_sink_raw_trackwise_deviations" "raw.trackwise.deviations" "trackwise"
create_connector "s3_sink_raw_sap_orders" "raw.sap.orders" "sap"
create_connector "s3_sink_raw_sop_documents" "raw.sop.documents" "sop"

echo "Connector statuses:"
curl -sS "${CONNECT_URL}/connectors?expand=status"
