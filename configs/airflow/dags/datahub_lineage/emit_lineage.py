"""
DataHub Lineage Emitter
Called by the medallion_pipeline DAG to emit pipeline lineage metadata
to DataHub GMS after each successful gold layer build.

Usage:
    python emit_lineage.py --gms-url http://datahub-gms:8080 --run-id <airflow_run_id>
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("datahub-lineage-emitter")

DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://datahub-gms:8080")
PLATFORM = "trino"
PLATFORM_INSTANCE = "enterprise-lakehouse"
ENV = "PROD"

# Medallion Lineage Edges
# Defines the Bronze → Silver → Gold lineage graph
LINEAGE_EDGES = [
    # Silver ← Bronze
    {
        "downstream": "silver.silver_mes_production_orders",
        "upstreams": ["bronze.mes_production_orders"],
    },
    {
        "downstream": "silver.silver_iqms_quality_tests",
        "upstreams": ["bronze.iqms_quality_tests"],
    },
    {
        "downstream": "silver.silver_iqms_deviations",
        "upstreams": ["bronze.iqms_deviations"],
    },
    {
        "downstream": "silver.silver_sap_inventory",
        "upstreams": ["bronze.sap_inventory_movements"],
    },
    {
        "downstream": "silver.silver_trackwise_capas",
        "upstreams": ["bronze.trackwise_capas"],
    },
    {
        "downstream": "silver.silver_tms_training",
        "upstreams": ["bronze.tms_training_completions"],
    },
    # Gold ← Silver
    {
        "downstream": "gold.gold_manufacturing_oee_mart",
        "upstreams": [
            "silver.silver_mes_production_orders",
            "silver.silver_iqms_quality_tests",
        ],
    },
    {
        "downstream": "gold.gold_quality_risk_mart",
        "upstreams": [
            "silver.silver_mes_production_orders",
            "silver.silver_iqms_quality_tests",
            "silver.silver_iqms_deviations",
        ],
    },
    {
        "downstream": "gold.gold_compliance_capa_mart",
        "upstreams": [
            "silver.silver_trackwise_capas",
            "silver.silver_iqms_deviations",
        ],
    },
    {
        "downstream": "gold.gold_sap_inventory_mart",
        "upstreams": ["silver.silver_sap_inventory"],
    },
    {
        "downstream": "gold.gold_supply_chain_mart",
        "upstreams": [
            "silver.silver_sap_inventory",
            "silver.silver_trackwise_capas",
        ],
    },
    {
        "downstream": "gold.gold_training_compliance_mart",
        "upstreams": ["silver.silver_tms_training"],
    },
]


def make_dataset_urn(schema_table: str) -> str:
    """Create a DataHub dataset URN from schema.table notation."""
    return (
        f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},"
        f"{PLATFORM_INSTANCE}.iceberg.{schema_table},{ENV})"
    )


def emit_lineage(gms_url: str, run_id: str):
    """Emit all lineage edges to DataHub GMS."""
    total = len(LINEAGE_EDGES)
    success = 0
    failed = 0

    logger.info(f"Emitting {total} lineage edges to {gms_url}")
    logger.info(f"Airflow run_id: {run_id}")

    for edge in LINEAGE_EDGES:
        downstream_urn = make_dataset_urn(edge["downstream"])
        upstream_urns = [make_dataset_urn(u) for u in edge["upstreams"]]

        payload = {
            "proposal": {
                "entityType": "dataset",
                "entityUrn": downstream_urn,
                "aspectName": "upstreamLineage",
                "changeType": "UPSERT",
                "aspect": {
                    "value": json.dumps({
                        "upstreams": [
                            {
                                "auditStamp": {
                                    "time": int(datetime.utcnow().timestamp() * 1000),
                                    "actor": "urn:li:corpuser:airflow",
                                },
                                "dataset": urn,
                                "type": "TRANSFORMED",
                            }
                            for urn in upstream_urns
                        ]
                    }),
                    "contentType": "application/json",
                },
            }
        }

        try:
            response = requests.post(
                f"{gms_url}/aspects?action=ingestProposal",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if response.status_code in (200, 201):
                success += 1
                logger.info(f"  ✅ {edge['downstream']} ← {edge['upstreams']}")
            else:
                failed += 1
                logger.warning(
                    f"  ❌ {edge['downstream']}: HTTP {response.status_code}"
                )
        except Exception as e:
            failed += 1
            logger.error(f"  ❌ {edge['downstream']}: {e}")

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Lineage emission complete: {success}/{total} success, {failed} failed")

    if failed > 0:
        logger.warning("Some lineage edges failed to emit — DataHub may not be running")
        # Don't fail the DAG for lineage emission failures
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="Emit medallion lineage to DataHub")
    parser.add_argument(
        "--gms-url", default=DATAHUB_GMS_URL, help="DataHub GMS REST URL"
    )
    parser.add_argument(
        "--run-id", default="manual", help="Airflow run ID for audit trail"
    )
    args = parser.parse_args()

    emit_lineage(args.gms_url, args.run_id)


if __name__ == "__main__":
    main()
