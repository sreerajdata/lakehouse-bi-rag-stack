"""
Enterprise Data Lakehouse — Document Indexer
Reads documents from SeaweedFS, extracts text, chunks, embeds, and upserts to Milvus.

Usage:
    python index_documents.py --bucket lakehouse-docs --collection enterprise_documents
    python index_documents.py --bucket lakehouse-docs --tika-url http://tika:9998
"""

import argparse
import io
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

import boto3
from botocore.config import Config as BotoConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("document-indexer")

# Configuration
S3_ENDPOINT = os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333")
S3_ACCESS_KEY = os.getenv("SEAWEEDFS_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("SEAWEEDFS_SECRET_KEY", "admin123")
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
DEFAULT_TIKA_URL = os.getenv("TIKA_URL", "http://tika:9998")


def get_s3_client():
    """Create boto3 S3 client pointing to SeaweedFS."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_documents(s3_client, bucket: str, prefix: str = "") -> List[Dict[str, Any]]:
    """List all documents in a SeaweedFS bucket."""
    documents = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(("/", ".checkpoint")):
                continue
            documents.append({
                "key": key,
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            })
    logger.info(f"Found {len(documents)} documents in s3://{bucket}/{prefix}")
    return documents


def extract_text_pdf(content: bytes) -> str:
    """Extract text from PDF using PyMuPDF (fitz)."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=content, filetype="pdf")
        text_parts = []
        for page_num, page in enumerate(doc):
            text = page.get_text("text")
            if text.strip():
                text_parts.append(f"[Page {page_num + 1}]\n{text}")
        doc.close()
        return "\n\n".join(text_parts)
    except Exception as e:
        logger.warning(f"PyMuPDF extraction failed: {e}")
        return ""


def extract_text_tika(content: bytes, tika_url: str, filename: str) -> str:
    """Extract text using Apache Tika REST API as fallback."""
    try:
        import httpx
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Tika-OCRLanguage": "eng",
        }
        response = httpx.put(
            f"{tika_url}/tika",
            content=content,
            headers=headers,
            timeout=120.0,
        )
        if response.status_code == 200:
            return response.text
        logger.warning(f"Tika returned status {response.status_code} for {filename}")
    except Exception as e:
        logger.warning(f"Tika extraction failed for {filename}: {e}")
    return ""


def extract_text_plain(content: bytes, filename: str) -> str:
    """Extract text directly from plain-text document types."""
    try:
        return content.decode("utf-8", errors="ignore")
    except Exception as e:
        logger.warning(f"Plain-text extraction failed for {filename}: {e}")
        return ""


def chunk_text(text: str, chunk_size: int = 512, overlap: int = 50) -> List[str]:
    """Split text into overlapping chunks by approximate token count."""
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = min(start + chunk_size, len(words))
        chunk = " ".join(words[start:end])
        if chunk.strip():
            chunks.append(chunk)
        start += chunk_size - overlap
    return chunks


def index_documents(
    bucket: str,
    collection: str = "enterprise_documents",
    tika_url: str = DEFAULT_TIKA_URL,
    prefix: str = "",
):
    """
    Main indexing pipeline:
    1. List docs from SeaweedFS
    2. Extract text (PyMuPDF for PDFs, Tika for others)
    3. Chunk into 512-token windows with 50-token overlap
    4. Generate embeddings via Ollama
    5. Upsert to Milvus
    """
    from langchain_community.embeddings import OllamaEmbeddings
    from langchain_community.vectorstores import Milvus

    s3 = get_s3_client()
    documents = list_documents(s3, bucket, prefix)

    if not documents:
        logger.info("No documents found to index.")
        return

    # Initialize embeddings
    embeddings = OllamaEmbeddings(base_url=OLLAMA_URL, model=EMBEDDING_MODEL)

    all_texts = []
    all_metadata = []

    for doc_info in documents:
        key = doc_info["key"]
        filename = key.split("/")[-1]
        logger.info(f"Processing: {filename} ({doc_info['size']} bytes)")

        try:
            # Download from SeaweedFS
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read()

            # Extract text
            lower_name = filename.lower()
            if lower_name.endswith(".pdf"):
                text = extract_text_pdf(content)
                extraction_method = "pymupdf"
                # Fall back to Tika if minimal text extracted (likely scanned PDF)
                if len(text.strip()) < 100:
                    logger.info(f"  Minimal text from PyMuPDF, trying Tika for {filename}")
                    text = extract_text_tika(content, tika_url, filename)
                    extraction_method = "tika_ocr"
            elif lower_name.endswith((".txt", ".md", ".csv", ".json", ".yml", ".yaml", ".log")):
                text = extract_text_plain(content, filename)
                extraction_method = "plain_text"
            else:
                text = extract_text_tika(content, tika_url, filename)
                extraction_method = "tika"

            if not text.strip():
                logger.warning(f"  No text extracted from {filename}, skipping.")
                continue

            # Chunk text
            chunks = chunk_text(text)
            logger.info(f"  Extracted {len(chunks)} chunks from {filename}")

            for i, chunk in enumerate(chunks):
                all_texts.append(chunk)
                all_metadata.append({
                    "filename": filename,
                    "source_key": key,
                    "source_system": "document_store",
                    "page_number": i + 1,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    "extraction_method": extraction_method,
                    "ingested_at": datetime.utcnow().isoformat(),
                    "bucket": bucket,
                })

        except Exception as e:
            logger.error(f"  Error processing {filename}: {e}")
            continue

    if not all_texts:
        logger.info("No text chunks to index.")
        return

    # Upsert to Milvus
    logger.info(f"Upserting {len(all_texts)} chunks to Milvus collection '{collection}'...")
    try:
        Milvus.from_texts(
            texts=all_texts,
            embedding=embeddings,
            metadatas=all_metadata,
            collection_name=collection,
            connection_args={"host": MILVUS_HOST, "port": MILVUS_PORT},
        )
        logger.info(f"✅ Successfully indexed {len(all_texts)} chunks into '{collection}'")
    except Exception as e:
        logger.error(f"❌ Milvus upsert failed: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Index documents from SeaweedFS to Milvus")
    parser.add_argument("--bucket", default="lakehouse-docs", help="SeaweedFS bucket name")
    parser.add_argument("--collection", default="enterprise_documents", help="Milvus collection name")
    parser.add_argument("--tika-url", default=DEFAULT_TIKA_URL, help="Tika REST API URL")
    parser.add_argument("--prefix", default="", help="S3 key prefix filter")
    args = parser.parse_args()

    index_documents(
        bucket=args.bucket,
        collection=args.collection,
        tika_url=args.tika_url,
        prefix=args.prefix,
    )
