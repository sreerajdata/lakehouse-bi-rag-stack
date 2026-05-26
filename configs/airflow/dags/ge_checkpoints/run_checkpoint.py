"""
Great Expectations Checkpoint Runner
Silver Layer DQ Validation

Usage:
    python run_checkpoint.py --checkpoint mes_silver_checkpoint
    python run_checkpoint.py --checkpoint iqms_silver_checkpoint
    python run_checkpoint.py --checkpoint sap_silver_checkpoint
"""

import argparse
import json
import os
import sys
from datetime import datetime

import yaml


def run_checkpoint(checkpoint_name: str) -> bool:
    """
    Run a Great Expectations checkpoint against Trino silver layer tables.
    
    Args:
        checkpoint_name: Name of the checkpoint YAML file (without extension)
        
    Returns:
        True if all expectations pass, False otherwise
    """
    checkpoint_dir = os.path.dirname(os.path.abspath(__file__))
    checkpoint_path = os.path.join(checkpoint_dir, f"{checkpoint_name}.yml")

    if not os.path.exists(checkpoint_path):
        print(f"ERROR: Checkpoint file not found: {checkpoint_path}")
        return False

    with open(checkpoint_path, "r") as f:
        checkpoint_config = yaml.safe_load(f)

    expectations = checkpoint_config.get("expectations", [])
    if not expectations:
        print(f"WARNING: No expectations found in {checkpoint_name}")
        return True

    try:
        from sqlalchemy import create_engine, text

        trino_host = os.getenv("TRINO_HOST", "trino")
        trino_port = os.getenv("TRINO_PORT", "8080")
        engine = create_engine(
            f"trino://admin@{trino_host}:{trino_port}/iceberg/silver"
        )
    except Exception as e:
        print(f"ERROR: Failed to connect to Trino: {e}")
        return False

    validations = checkpoint_config.get("validations", [])
    if validations:
        data_asset = validations[0].get("batch_request", {}).get(
            "data_asset_name", ""
        )
        table_name = data_asset.split(".")[-1] if data_asset else checkpoint_name.replace("_checkpoint", "")
    else:
        table_name = checkpoint_name.replace("_checkpoint", "")

    results = {
        "checkpoint_name": checkpoint_name,
        "run_time": datetime.utcnow().isoformat(),
        "table": table_name,
        "expectations": [],
        "success": True,
    }

    with engine.connect() as conn:
        for exp in expectations:
            exp_type = exp.get("expectation_type", "")
            kwargs = exp.get("kwargs", {})
            column = kwargs.get("column", "")
            exp_result = {"type": exp_type, "column": column, "success": True, "details": ""}

            try:
                if exp_type == "expect_column_values_to_not_be_null":
                    row = conn.execute(
                        text(f"SELECT COUNT(*) AS total, "
                             f"COUNT({column}) AS non_null "
                             f"FROM {table_name}")
                    ).fetchone()
                    total, non_null = row[0], row[1]
                    null_pct = (total - non_null) / max(total, 1)
                    mostly = kwargs.get("mostly", 1.0)
                    exp_result["success"] = null_pct <= (1 - mostly)
                    exp_result["details"] = f"{non_null}/{total} non-null ({(1-null_pct)*100:.1f}%)"

                elif exp_type == "expect_column_values_to_be_between":
                    min_val = kwargs.get("min_value")
                    max_val = kwargs.get("max_value")
                    conditions = []
                    if min_val is not None:
                        op = ">" if kwargs.get("strictly") else ">="
                        conditions.append(f"{column} {op} {min_val}")
                    if max_val is not None:
                        conditions.append(f"{column} <= {max_val}")
                    where_clause = " AND ".join(conditions) if conditions else "1=1"
                    row = conn.execute(
                        text(f"SELECT COUNT(*) AS total, "
                             f"SUM(CASE WHEN {where_clause} THEN 1 ELSE 0 END) AS valid "
                             f"FROM {table_name} WHERE {column} IS NOT NULL")
                    ).fetchone()
                    total, valid = row[0], row[1]
                    mostly = kwargs.get("mostly", 1.0)
                    pct = valid / max(total, 1)
                    exp_result["success"] = pct >= mostly
                    exp_result["details"] = f"{valid}/{total} in range ({pct*100:.1f}%)"

                elif exp_type == "expect_column_values_to_be_in_set":
                    value_set = kwargs.get("value_set", [])
                    values_str = ", ".join([f"'{v}'" for v in value_set])
                    row = conn.execute(
                        text(f"SELECT COUNT(*) AS total, "
                             f"SUM(CASE WHEN {column} IN ({values_str}) THEN 1 ELSE 0 END) AS valid "
                             f"FROM {table_name} WHERE {column} IS NOT NULL")
                    ).fetchone()
                    total, valid = row[0], row[1]
                    exp_result["success"] = total == valid
                    exp_result["details"] = f"{valid}/{total} in allowed set"

                elif exp_type == "expect_column_values_to_be_unique":
                    row = conn.execute(
                        text(f"SELECT COUNT({column}) AS total, "
                             f"COUNT(DISTINCT {column}) AS distinct_count "
                             f"FROM {table_name}")
                    ).fetchone()
                    total, distinct_count = row[0], row[1]
                    mostly = kwargs.get("mostly", 1.0)
                    pct = distinct_count / max(total, 1)
                    exp_result["success"] = pct >= mostly
                    exp_result["details"] = f"{distinct_count}/{total} unique ({pct*100:.1f}%)"

                else:
                    exp_result["details"] = f"Unsupported expectation type: {exp_type}"

            except Exception as e:
                exp_result["success"] = False
                exp_result["details"] = f"Error: {str(e)}"

            if not exp_result["success"]:
                results["success"] = False
            results["expectations"].append(exp_result)

    status = "✅ PASSED" if results["success"] else "❌ FAILED"
    print(f"\n{'='*60}")
    print(f"Checkpoint: {checkpoint_name} — {status}")
    print(f"Table: {table_name}")
    print(f"Run Time: {results['run_time']}")
    print(f"{'='*60}")
    for exp in results["expectations"]:
        icon = "✅" if exp["success"] else "❌"
        print(f"  {icon} {exp['type']} ({exp['column']}): {exp['details']}")

    try:
        import boto3
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333"),
            aws_access_key_id=os.getenv("SEAWEEDFS_ACCESS_KEY", "admin"),
            aws_secret_access_key=os.getenv("SEAWEEDFS_SECRET_KEY", "admin123"),
        )
        result_key = (
            f"dq_results/{checkpoint_name}/"
            f"{datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')}.json"
        )
        s3.put_object(
            Bucket="lakehouse-gold",
            Key=result_key,
            Body=json.dumps(results, indent=2, default=str),
            ContentType="application/json",
        )
        print(f"\n  📤 Results saved to s3a://lakehouse-gold/{result_key}")
    except Exception as e:
        print(f"\n  ⚠️  Could not save results to S3: {e}")

    return results["success"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Great Expectations checkpoint")
    parser.add_argument(
        "--checkpoint", required=True,
        help="Checkpoint name (e.g., mes_silver_checkpoint)"
    )
    args = parser.parse_args()

    success = run_checkpoint(args.checkpoint)
    sys.exit(0 if success else 1)
