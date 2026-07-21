"""
re_index_rag.py — Trigger RAG document re-indexing on demand.

Run from the host:
    docker exec lakehouse_langchain_app python /app/re_index_rag.py

Or from outside the container (Windows):
    docker exec lakehouse_langchain_app python /app/re_index_rag.py

The script:
  1. Uploads any new files from the /workspace/docs folder to SeaweedFS
  2. Re-indexes all documents in the lakehouse-docs bucket into Milvus

Use this whenever you add new docs and want the RAG pipeline to pick them up.
"""
import os
import sys
import time
from pathlib import Path

# Re-use the existing index_documents utility already inside the container
sys.path.insert(0, "/app")

OLLAMA_URL        = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
EMBEDDING_MODEL   = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
MILVUS_HOST       = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT       = int(os.getenv("MILVUS_PORT", "19530"))
COLLECTION_NAME   = os.getenv("MILVUS_COLLECTION", "enterprise_documents")
DOCS_BUCKET       = os.getenv("DOCS_BUCKET", "lakehouse-docs")
SEAWEEDFS_ENDPOINT = os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333")
SEAWEEDFS_ACCESS_KEY = os.getenv("SEAWEEDFS_ACCESS_KEY", "admin")
SEAWEEDFS_SECRET_KEY = os.getenv("SEAWEEDFS_SECRET_KEY", "admin123")
TIKA_URL          = os.getenv("TIKA_URL", "http://tika:9998")

# Additional docs to upload on re-index (add paths here as needed)
NEW_DOCS = [
    Path("/workspace/docs"),          # entire directory
    Path("/workspace/README.md"),
]

import boto3
from botocore.config import Config as BotoConfig
from utils.index_documents import index_documents


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=SEAWEEDFS_ENDPOINT,
        aws_access_key_id=SEAWEEDFS_ACCESS_KEY,
        aws_secret_access_key=SEAWEEDFS_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_docs():
    s3 = get_s3()
    uploaded = []
    for path in NEW_DOCS:
        if path.is_dir():
            files = list(path.rglob("*"))
        elif path.is_file():
            files = [path]
        else:
            print(f"[SKIP] Path not found: {path}")
            continue
        for f in files:
            if not f.is_file():
                continue
            suffix = f.suffix.lower()
            ct = "text/plain; charset=utf-8" if suffix in {".md", ".txt", ".csv"} else "application/octet-stream"
            key = f.name
            s3.put_object(Bucket=DOCS_BUCKET, Key=key, Body=f.read_bytes(), ContentType=ct)
            uploaded.append(key)
            print(f"  ✅ Uploaded: {key}")
    return uploaded


if __name__ == "__main__":
    print("=" * 60)
    print("  RAG Re-Indexing — Enterprise Lakehouse")
    print("=" * 60)
    print(f"\n[1/3] Uploading docs to s3://{DOCS_BUCKET} ...")
    uploaded = upload_docs()
    print(f"  → {len(uploaded)} files uploaded\n")

    print("[2/3] Re-indexing into Milvus collection: " + COLLECTION_NAME)
    print(f"  → Drop & recreate collection to avoid duplicates\n")

    try:
        from pymilvus import connections, utility
        connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
        if utility.has_collection(COLLECTION_NAME):
            utility.drop_collection(COLLECTION_NAME)
            print(f"  → Dropped existing collection '{COLLECTION_NAME}'")
        connections.disconnect("default")
    except Exception as e:
        print(f"  [WARN] Could not drop collection: {e}")

    index_documents(bucket=DOCS_BUCKET, collection=COLLECTION_NAME, tika_url=TIKA_URL)

    print("\n[3/3] Done!")
    print(f"  RAG pipeline re-indexed. {len(uploaded)} docs now queryable at http://localhost:8501")
    print("=" * 60)
