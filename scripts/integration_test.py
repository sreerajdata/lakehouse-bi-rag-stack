"""
End-to-End Integration Test Suite
Tests all platform components: SeaweedFS, Kafka, Trino, Airflow, Spark, Ollama, Milvus, Grafana.

Usage:
    python integration_test.py
    make integration-test
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime
from typing import Tuple

# Configuration
SEAWEEDFS_S3_URL = os.getenv("SEAWEEDFS_ENDPOINT", "http://localhost:8333")
SEAWEEDFS_KEY    = os.getenv("SEAWEEDFS_ACCESS_KEY", "admin")
SEAWEEDFS_SECRET = os.getenv("SEAWEEDFS_SECRET_KEY", "admin123")
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TRINO_HOST       = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT       = int(os.getenv("TRINO_PORT", "8180"))
AIRFLOW_URL      = os.getenv("AIRFLOW_URL", "http://localhost:8280")
OLLAMA_URL       = os.getenv("OLLAMA_URL", "http://localhost:11434")
MILVUS_HOST      = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT      = int(os.getenv("MILVUS_PORT", "19530"))
GRAFANA_URL      = os.getenv("GRAFANA_URL", "http://localhost:3000")
SPARK_URL        = os.getenv("SPARK_URL", "http://localhost:8181")


class TestResult:
    """Holds result of a single test."""
    def __init__(self, name: str, status: str, message: str, duration: float):
        self.name = name
        self.status = status  # "PASS", "FAIL", "SKIP"
        self.message = message
        self.duration = duration

    def to_dict(self):
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "duration_ms": round(self.duration * 1000, 1),
        }


def run_test(name: str, fn) -> TestResult:
    """Execute a single test function with timing."""
    start = time.time()
    try:
        success, message = fn()
        duration = time.time() - start
        return TestResult(name, "PASS" if success else "FAIL", message, duration)
    except Exception as e:
        duration = time.time() - start
        return TestResult(name, "FAIL", f"Exception: {str(e)}", duration)


# Test Functions

def test_seaweedfs() -> Tuple[bool, str]:
    """Test SeaweedFS S3 connectivity and bucket operations."""
    import boto3
    s3 = boto3.client(
        "s3", endpoint_url=SEAWEEDFS_S3_URL,
        aws_access_key_id=SEAWEEDFS_KEY,
        aws_secret_access_key=SEAWEEDFS_SECRET,
    )
    # List buckets
    buckets = s3.list_buckets()
    bucket_names = [b["Name"] for b in buckets["Buckets"]]
    expected = ["lakehouse-bronze", "lakehouse-silver", "lakehouse-gold"]
    missing = [b for b in expected if b not in bucket_names]
    if missing:
        return False, f"Missing buckets: {missing}. Found: {bucket_names}"

    # Write/read test
    test_key = "_integration_test/test.json"
    test_data = json.dumps({"test": True, "ts": datetime.utcnow().isoformat()})
    s3.put_object(Bucket="lakehouse-bronze", Key=test_key, Body=test_data)
    response = s3.get_object(Bucket="lakehouse-bronze", Key=test_key)
    read_data = response["Body"].read().decode()
    s3.delete_object(Bucket="lakehouse-bronze", Key=test_key)
    if json.loads(read_data).get("test") != True:
        return False, "Write/read round-trip failed"

    return True, f"Buckets OK ({len(bucket_names)} found). Write/read OK."


def test_kafka() -> Tuple[bool, str]:
    """Test Kafka broker connectivity and topic listing."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        consumer_timeout_ms=5000,
    )
    topics = consumer.topics()
    consumer.close()
    if not topics:
        return False, "No topics found"
    return True, f"Connected. {len(topics)} topics available."


def test_trino_connectivity() -> Tuple[bool, str]:
    """Test Trino query engine connectivity."""
    import httpx
    response = httpx.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info", timeout=10.0)
    if response.status_code != 200:
        return False, f"Trino returned {response.status_code}"
    info = response.json()
    return True, f"Trino {info.get('nodeVersion', {}).get('version', 'unknown')} running"


def test_trino_iceberg() -> Tuple[bool, str]:
    """Test Trino Iceberg catalog — list schemas."""
    import httpx
    # POST a query to list schemas
    headers = {"X-Trino-User": "admin", "X-Trino-Catalog": "iceberg"}
    response = httpx.post(
        f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement",
        content="SHOW SCHEMAS FROM iceberg",
        headers=headers,
        timeout=30.0,
    )
    if response.status_code != 200:
        return False, f"Trino query failed: {response.status_code}"
    return True, f"Iceberg catalog accessible via Trino"


def test_airflow() -> Tuple[bool, str]:
    """Test Airflow web server and DAG listing."""
    import httpx
    response = httpx.get(
        f"{AIRFLOW_URL}/api/v1/dags",
        auth=("admin", "admin"),
        timeout=15.0,
    )
    if response.status_code != 200:
        return False, f"Airflow API returned {response.status_code}"
    dags = response.json().get("dags", [])
    dag_ids = [d["dag_id"] for d in dags]
    return True, f"{len(dags)} DAGs found: {dag_ids[:5]}"


def test_spark() -> Tuple[bool, str]:
    """Test Spark master UI accessibility."""
    import httpx
    response = httpx.get(f"{SPARK_URL}", timeout=10.0)
    if response.status_code != 200:
        return False, f"Spark master UI returned {response.status_code}"
    return True, "Spark master UI accessible"


def test_ollama() -> Tuple[bool, str]:
    """Test Ollama LLM server and model availability."""
    import httpx
    response = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=15.0)
    if response.status_code != 200:
        return False, f"Ollama returned {response.status_code}"
    models = response.json().get("models", [])
    model_names = [m["name"] for m in models]
    if not model_names:
        return False, "No models loaded in Ollama"
    return True, f"Models available: {model_names}"


def test_milvus() -> Tuple[bool, str]:
    """Test Milvus vector store connectivity."""
    import httpx
    response = httpx.get(f"http://{MILVUS_HOST}:{MILVUS_PORT - 10439}/healthz", timeout=10.0)
    # Milvus health endpoint is on port 9091
    try:
        response = httpx.get(f"http://{MILVUS_HOST}:9091/healthz", timeout=10.0)
        if response.status_code == 200:
            return True, "Milvus is healthy"
    except Exception:
        pass
    # Fallback: try to connect via pymilvus
    try:
        from pymilvus import connections
        connections.connect(host=MILVUS_HOST, port=str(MILVUS_PORT))
        connections.disconnect("default")
        return True, "Milvus connected via pymilvus"
    except ImportError:
        return False, "pymilvus not installed, cannot verify Milvus"
    except Exception as e:
        return False, f"Milvus connection failed: {e}"


def test_grafana() -> Tuple[bool, str]:
    """Test Grafana API and dashboard provisioning."""
    import httpx
    response = httpx.get(
        f"{GRAFANA_URL}/api/search",
        auth=("admin", "admin123"),
        timeout=10.0,
    )
    if response.status_code != 200:
        return False, f"Grafana API returned {response.status_code}"
    dashboards = response.json()
    return True, f"{len(dashboards)} dashboards provisioned"


def test_prometheus() -> Tuple[bool, str]:
    """Test Prometheus health and active targets."""
    import httpx
    response = httpx.get("http://localhost:9090/api/v1/targets", timeout=10.0)
    if response.status_code != 200:
        return False, f"Prometheus returned {response.status_code}"
    targets = response.json().get("data", {}).get("activeTargets", [])
    up_targets = [t for t in targets if t.get("health") == "up"]
    return True, f"{len(up_targets)}/{len(targets)} targets UP"


# Main Test Runner

def main():
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  End-to-End Integration Test Suite                      ║")
    print("║  Testing all platform components...                     ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"  Timestamp: {datetime.utcnow().isoformat()}")
    print()

    tests = [
        ("SeaweedFS S3 Storage",    test_seaweedfs),
        ("Kafka Messaging",         test_kafka),
        ("Trino Connectivity",      test_trino_connectivity),
        ("Trino Iceberg Catalog",   test_trino_iceberg),
        ("Airflow Orchestration",   test_airflow),
        ("Spark Processing",        test_spark),
        ("Ollama LLM",              test_ollama),
        ("Milvus Vector Store",     test_milvus),
        ("Grafana Dashboards",      test_grafana),
        ("Prometheus Monitoring",   test_prometheus),
    ]

    results = []
    for name, fn in tests:
        result = run_test(name, fn)
        icon = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️"}.get(result.status, "❓")
        print(f"  {icon} {result.name:30s} [{result.duration*1000:7.1f}ms] {result.message}")
        results.append(result)

    # Summary
    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    total = len(results)

    print()
    print(f"{'='*60}")
    print(f"  Results: {passed}/{total} PASSED, {failed} FAILED")
    print(f"  Total Duration: {sum(r.duration for r in results)*1000:.0f}ms")
    print(f"{'='*60}")

    # Write JUnit XML
    junit_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_results.xml")
    with open(junit_path, "w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(f'<testsuite name="Lakehouse-Integration" tests="{total}" '
                f'failures="{failed}" timestamp="{datetime.utcnow().isoformat()}">\n')
        for r in results:
            f.write(f'  <testcase name="{r.name}" time="{r.duration:.3f}">\n')
            if r.status == "FAIL":
                f.write(f'    <failure message="{r.message}"/>\n')
            f.write(f'  </testcase>\n')
        f.write('</testsuite>\n')
    print(f"\n  📄 JUnit XML: {junit_path}")

    # Write JSON results
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_results.json")
    with open(json_path, "w") as f:
        json.dump({
            "suite": "Lakehouse-Integration",
            "timestamp": datetime.utcnow().isoformat(),
            "total": total,
            "passed": passed,
            "failed": failed,
            "results": [r.to_dict() for r in results],
        }, f, indent=2)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
