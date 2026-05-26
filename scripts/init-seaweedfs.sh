#!/bin/sh
set -e

ENDPOINT="http://seaweedfs-s3:8333"
BUCKETS="bronze silver gold lakehouse-bronze lakehouse-silver lakehouse-gold lakehouse-models lakehouse-docs milvus-bucket"

echo "Waiting for SeaweedFS S3 to be ready..."
until aws --endpoint-url=$ENDPOINT s3 ls > /dev/null 2>&1; do
  echo "Waiting..."
  sleep 5
done

echo "SeaweedFS S3 is ready. Creating buckets..."
for BUCKET in $BUCKETS; do
  if aws --endpoint-url=$ENDPOINT s3 ls "s3://$BUCKET" > /dev/null 2>&1; then
    echo "Bucket $BUCKET already exists."
  else
    aws --endpoint-url=$ENDPOINT s3 mb "s3://$BUCKET"
    echo "Created bucket: $BUCKET"
  fi
done

for SYSTEM in mes iqms historian trackwise sap tms nifi_flows docs; do
  aws --endpoint-url=$ENDPOINT s3api put-object --bucket bronze --key "${SYSTEM}/" > /dev/null 2>&1 || true
  aws --endpoint-url=$ENDPOINT s3api put-object --bucket lakehouse-bronze --key "${SYSTEM}/" > /dev/null 2>&1 || true
done

for LAYER in bronze silver gold; do
  aws --endpoint-url=$ENDPOINT s3api put-object --bucket "lakehouse-$LAYER" --key "warehouse/" > /dev/null 2>&1 || true
done

echo "Bucket initialization complete."
