#!/bin/bash
# ============================================================
# Kafka Connect — Debezium Connector Registration Script
# TPL Data Lakehouse
# Usage: ./scripts/register_connectors.sh
#        or: make register-connectors
# ============================================================
set -e
set -u

CONNECT_URL="${KAFKA_CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_DIR="${CONNECTOR_DIR:-./configs/kafka-connect/connectors}"

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  TPL Lakehouse — Kafka Connect Connector Registration   ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Connect URL: ${CONNECT_URL}"
echo "Connector Dir: ${CONNECTOR_DIR}"
echo ""

# Wait for Kafka Connect to be available
echo "⏳ Waiting for Kafka Connect to be ready..."
MAX_RETRIES=30
RETRY=0
until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
    RETRY=$((RETRY + 1))
    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo "❌ Kafka Connect not available after ${MAX_RETRIES} retries. Exiting."
        exit 1
    fi
    echo "   Waiting... (${RETRY}/${MAX_RETRIES})"
    sleep 5
done
echo "✅ Kafka Connect is ready."
echo ""

# Register each connector
TOTAL=0
SUCCESS=0
SKIPPED=0
FAILED=0

for CONNECTOR_FILE in "${CONNECTOR_DIR}"/*.json; do
    if [ ! -f "$CONNECTOR_FILE" ]; then
        echo "⚠️  No connector JSON files found in ${CONNECTOR_DIR}"
        exit 0
    fi

    CONNECTOR_NAME=$(python3 -c "import json; print(json.load(open('${CONNECTOR_FILE}'))['name'])" 2>/dev/null || basename "$CONNECTOR_FILE" .json)
    TOTAL=$((TOTAL + 1))

    echo "─────────────────────────────────────────"
    echo "📋 Connector: ${CONNECTOR_NAME}"
    echo "   File: ${CONNECTOR_FILE}"

    # Check if connector already exists
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")

    if [ "$HTTP_CODE" = "200" ]; then
        echo "   ⏭️  Already exists — updating configuration..."
        RESPONSE=$(curl -s -w "\n%{http_code}" -X PUT \
            -H "Content-Type: application/json" \
            -d @<(python3 -c "import json; data=json.load(open('${CONNECTOR_FILE}')); print(json.dumps(data.get('config', data)))") \
            "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config")
        STATUS_CODE=$(echo "$RESPONSE" | tail -1)
        if [ "$STATUS_CODE" = "200" ] || [ "$STATUS_CODE" = "201" ]; then
            echo "   ✅ Updated successfully"
            SUCCESS=$((SUCCESS + 1))
        else
            echo "   ❌ Update failed (HTTP ${STATUS_CODE})"
            FAILED=$((FAILED + 1))
        fi
    else
        echo "   🆕 Creating new connector..."
        RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d @"${CONNECTOR_FILE}" \
            "${CONNECT_URL}/connectors")
        STATUS_CODE=$(echo "$RESPONSE" | tail -1)
        if [ "$STATUS_CODE" = "201" ] || [ "$STATUS_CODE" = "200" ]; then
            echo "   ✅ Created successfully"
            SUCCESS=$((SUCCESS + 1))
        else
            echo "   ❌ Creation failed (HTTP ${STATUS_CODE})"
            echo "   Response: $(echo "$RESPONSE" | head -1)"
            FAILED=$((FAILED + 1))
        fi
    fi
done

echo ""
echo "═══════════════════════════════════════════"
echo "📊 Results: ${SUCCESS} success, ${FAILED} failed, ${SKIPPED} skipped (${TOTAL} total)"
echo ""

# List all active connectors
echo "📡 Active Connectors:"
curl -s "${CONNECT_URL}/connectors" | python3 -m json.tool 2>/dev/null || curl -s "${CONNECT_URL}/connectors"
echo ""

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
