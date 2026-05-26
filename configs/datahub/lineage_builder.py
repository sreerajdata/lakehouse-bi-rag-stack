"""
DataHub Lineage Builder
Reads dbt manifest.json, extracts model dependencies, and emits
DatasetLineageClass events to DataHub REST API.

Usage:
    python lineage_builder.py --manifest ./dbt/target/manifest.json
    python lineage_builder.py --manifest ./dbt/target/manifest.json --dry-run
"""

import argparse
import json
import logging
import os
import sys
import requests
from typing import Dict, List, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("lineage-builder")

DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://datahub-gms:8080")
PLATFORM = "trino"
PLATFORM_INSTANCE = "enterprise-lakehouse"
ENV = "PROD"


def load_manifest(manifest_path: str) -> Dict[str, Any]:
    """Load dbt manifest.json."""
    with open(manifest_path, "r") as f:
        return json.load(f)


def extract_lineage(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract upstream→downstream lineage edges from dbt manifest.

    Returns a list of lineage proposals:
        [{downstream: str, upstreams: [str]}]
    """
    lineage_edges = []
    nodes = manifest.get("nodes", {})

    for node_id, node in nodes.items():
        if node.get("resource_type") not in ("model", "snapshot"):
            continue

        schema = node.get("schema", "default")
        name = node.get("name", node_id.split(".")[-1])
        downstream_urn = make_dataset_urn(schema, name)

        upstreams = []
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            dep_node = nodes.get(dep_id) or manifest.get("sources", {}).get(dep_id)
            if dep_node:
                dep_schema = dep_node.get("schema", "default")
                dep_name = dep_node.get("name", dep_id.split(".")[-1])
                upstreams.append(make_dataset_urn(dep_schema, dep_name))
            else:
                parts = dep_id.split(".")
                if len(parts) >= 3:
                    upstreams.append(make_dataset_urn(parts[-2], parts[-1]))

        if upstreams:
            lineage_edges.append({
                "downstream": downstream_urn,
                "upstreams": upstreams,
                "model_name": name,
                "schema": schema,
            })

    return lineage_edges


def make_dataset_urn(schema: str, table: str) -> str:
    """Create a DataHub dataset URN."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{PLATFORM_INSTANCE}.iceberg.{schema}.{table},{ENV})"


def emit_lineage(lineage_edges: List[Dict[str, Any]], dry_run: bool = False):
    """Emit lineage edges to DataHub GMS REST API."""
    total = len(lineage_edges)
    success = 0
    failed = 0

    for edge in lineage_edges:
        downstream = edge["downstream"]
        upstreams = edge["upstreams"]

        logger.info(f"Lineage: {edge['model_name']} <- {len(upstreams)} upstreams")
        for up in upstreams:
            logger.info(f"  ← {up}")

        if dry_run:
            success += 1
            continue

        payload = {
            "proposal": {
                "entityType": "dataset",
                "entityUrn": downstream,
                "aspectName": "upstreamLineage",
                "changeType": "UPSERT",
                "aspect": {
                    "value": json.dumps({
                        "upstreams": [
                            {
                                "auditStamp": {
                                    "time": 0,
                                    "actor": "urn:li:corpuser:datahub"
                                },
                                "dataset": up,
                                "type": "TRANSFORMED"
                            }
                            for up in upstreams
                        ]
                    }),
                    "contentType": "application/json"
                }
            }
        }

        try:
            response = requests.post(
                f"{DATAHUB_GMS_URL}/aspects?action=ingestProposal",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if response.status_code in (200, 201):
                success += 1
            else:
                logger.warning(f"Failed to emit lineage for {edge['model_name']}: {response.status_code}")
                failed += 1
        except Exception as e:
            logger.error(f"Error emitting lineage for {edge['model_name']}: {e}")
            failed += 1

    logger.info(f"\n{'='*50}")
    logger.info(f"Lineage emission complete: {success}/{total} success, {failed} failed")
    if dry_run:
        logger.info("(DRY RUN — no actual API calls made)")


def main():
    global DATAHUB_GMS_URL
    parser = argparse.ArgumentParser(description="Build and emit dbt lineage to DataHub")
    parser.add_argument("--manifest", required=True, help="Path to dbt manifest.json")
    parser.add_argument("--dry-run", action="store_true", help="Print lineage without emitting")
    parser.add_argument("--gms-url", default=DATAHUB_GMS_URL, help="DataHub GMS URL")
    args = parser.parse_args()

    DATAHUB_GMS_URL = args.gms_url

    logger.info(f"Loading manifest from {args.manifest}")
    manifest = load_manifest(args.manifest)
    logger.info(f"Found {len(manifest.get('nodes', {}))} nodes in manifest")

    lineage_edges = extract_lineage(manifest)
    logger.info(f"Extracted {len(lineage_edges)} lineage edges")

    emit_lineage(lineage_edges, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
