import os
import time
from pathlib import Path

import boto3
import httpx
from botocore.config import Config as BotoConfig

from utils.index_documents import index_documents

OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
DOCS_BUCKET = os.getenv("DOCS_BUCKET", "lakehouse-docs")
TIKA_URL = os.getenv("TIKA_URL", "http://tika:9998")
COLLECTION_NAME = os.getenv("MILVUS_COLLECTION", "enterprise_documents")
SEAWEEDFS_ENDPOINT = os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333")
SEAWEEDFS_ACCESS_KEY = os.getenv("SEAWEEDFS_ACCESS_KEY", "admin")
SEAWEEDFS_SECRET_KEY = os.getenv("SEAWEEDFS_SECRET_KEY", "admin123")
SEED_FILES = [
    Path("/workspace/README.md"),
    Path("/workspace/docs/STACK_EXECUTION_GUIDE.md"),
    Path("/workspace/docs/IMPLEMENTATION_PLAN.md"),
    Path("/workspace/data/source/mes_events.csv"),
    Path("/workspace/data/source/trackwise_deviations.csv"),
    Path("/workspace/data/source/sop_documents.csv"),
    Path("/workspace/data/source/sap_ecc_orders.csv"),
]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=SEAWEEDFS_ENDPOINT,
        aws_access_key_id=SEAWEEDFS_ACCESS_KEY,
        aws_secret_access_key=SEAWEEDFS_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_embedding_model() -> None:
    for _ in range(30):
        try:
            response = httpx.post(f"{OLLAMA_URL}/api/pull", json={"name": EMBEDDING_MODEL}, timeout=300.0)
            response.raise_for_status()
            return
        except Exception as exc:
            print(f"Waiting for Ollama embedding model readiness: {exc}")
            time.sleep(5)
    raise RuntimeError(f"Could not prepare Ollama embedding model {EMBEDDING_MODEL}")


def seed_bucket() -> list[str]:
    s3 = get_s3_client()
    existing_buckets = {bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])}
    if DOCS_BUCKET not in existing_buckets:
        s3.create_bucket(Bucket=DOCS_BUCKET)

    uploaded = []
    for path in SEED_FILES:
        if not path.exists() or not path.is_file():
            continue
        content_type = "text/plain; charset=utf-8" if path.suffix.lower() in {".md", ".txt", ".csv"} else "application/octet-stream"
        s3.put_object(Bucket=DOCS_BUCKET, Key=path.name, Body=path.read_bytes(), ContentType=content_type)
        uploaded.append(path.name)
    return uploaded


print("Preparing embedding model...")
ensure_embedding_model()
uploaded_files = seed_bucket()
print(f"Seeded {len(uploaded_files)} files into s3://{DOCS_BUCKET}: {uploaded_files}")
print("Starting index_documents...")
index_documents(bucket=DOCS_BUCKET, collection=COLLECTION_NAME, tika_url=TIKA_URL)
print("Finished indexing.")
