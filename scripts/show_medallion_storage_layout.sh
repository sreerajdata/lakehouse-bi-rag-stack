#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENDPOINT="http://seaweedfs-s3:8333"
AWS_IMAGE="amazon/aws-cli:2.15.0"
NETWORK="lakehouse-base-build_lakehouse_net"

list_bucket() {
  local bucket="$1"
  echo "=== ${bucket} ==="
  docker run --rm --network "$NETWORK" \
    -e AWS_ACCESS_KEY_ID=admin \
    -e AWS_SECRET_ACCESS_KEY=admin123 \
    "$AWS_IMAGE" \
    --endpoint-url "$ENDPOINT" s3 ls "s3://${bucket}/warehouse/" --recursive 2>/dev/null | head -n 20 || true
  echo ""
}

list_bucket "lakehouse-bronze"
list_bucket "lakehouse-silver"
list_bucket "lakehouse-gold"
