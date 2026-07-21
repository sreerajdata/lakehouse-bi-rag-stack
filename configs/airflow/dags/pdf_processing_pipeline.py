"""
Process PDFs from SeaweedFS through Tika, store extracted text in Bronze,
and index the Bronze document artifacts into Milvus.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
import subprocess

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


S3_ENDPOINT = os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333")
S3_ACCESS_KEY = os.getenv("SEAWEEDFS_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("SEAWEEDFS_SECRET_KEY", "admin123")
TIKA_URL = os.getenv("TIKA_URL", "http://tika:9998")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
MILVUS_INDEXER_CONTAINER = os.getenv("MILVUS_INDEXER_CONTAINER", "lakehouse_langchain_app")
MILVUS_COLLECTION = os.getenv("MILVUS_COLLECTION", "enterprise_documents")
TESSERACT_CONTAINER = os.getenv("TESSERACT_CONTAINER", "lakehouse_tesseract")

SOURCE_BUCKET = "lakehouse-docs"
SOURCE_PREFIX = "incoming/"
PROCESSED_PREFIX = "processed/"
BRONZE_BUCKET = "lakehouse-bronze"
BRONZE_PREFIX = "pdf_extractions/"
DLQ_PREFIX = "dlq/pdf/"


def _ocr_with_tesseract(pdf_bytes: bytes, filename: str) -> str:
    """
    Fallback OCR using the lakehouse_tesseract container.
    Steps:
      1. Write PDF bytes to a temp file inside the container via docker cp.
      2. Run `tesseract <input> stdout pdf` to extract text.
      3. Return extracted text, or empty string on any failure.
    Called only when both Tika passes return empty text.
    """
    import tempfile
    import uuid

    tmp_id = uuid.uuid4().hex[:8]
    container_input = f"/tmp/ocr_{tmp_id}.pdf"

    try:
        # Write PDF into the Tesseract container
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        cp_result = subprocess.run(
            ["docker", "cp", tmp_path, f"{TESSERACT_CONTAINER}:{container_input}"],
            capture_output=True, text=True, timeout=30,
        )
        if cp_result.returncode != 0:
            print(f"[Tesseract] docker cp failed: {cp_result.stderr}")
            return ""

        # Run OCR — tesseract writes to stdout when output is 'stdout'
        ocr_result = subprocess.run(
            ["docker", "exec", TESSERACT_CONTAINER,
             "tesseract", container_input, "stdout", "--psm", "1", "pdf"],
            capture_output=True, text=True, timeout=120,
        )
        text = ocr_result.stdout.strip()
        if not text and ocr_result.stderr:
            print(f"[Tesseract] stderr: {ocr_result.stderr[:300]}")
        return text

    except Exception as exc:
        print(f"[Tesseract] OCR failed for {filename}: {exc}")
        return ""
    finally:
        # Cleanup temp file in container (best-effort)
        subprocess.run(
            ["docker", "exec", TESSERACT_CONTAINER, "rm", "-f", container_input],
            capture_output=True, timeout=10,
        )
        try:
            import os as _os
            _os.unlink(tmp_path)
        except Exception:
            pass

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="pdf_processing_pipeline",
    default_args=default_args,
    description="PDF Processing Pipeline: SeaweedFS -> Tika -> Bronze -> Milvus",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["pdf", "document", "bronze", "rag"],
)


def _get_s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def _sql_string(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _write_bronze_rows(rows):
    if not rows:
        return

    import trino

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="bronze",
    )
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pdf_extractions (
            filename VARCHAR,
            source_key VARCHAR,
            bronze_key VARCHAR,
            extracted_text VARCHAR,
            char_count INTEGER,
            word_count INTEGER,
            extraction_method VARCHAR,
            extracted_at TIMESTAMP,
            source_system VARCHAR
        )
        WITH (
            format = 'PARQUET',
            format_version = 2
        )
        """
    )

    values = []
    for row in rows:
        extracted_at = row["extracted_at"].replace("T", " ")[:26]
        values.append(
            "("
            f"{_sql_string(row['filename'])}, "
            f"{_sql_string(row['source_key'])}, "
            f"{_sql_string(row['bronze_key'])}, "
            f"{_sql_string(row['extracted_text'])}, "
            f"{int(row['char_count'])}, "
            f"{int(row['word_count'])}, "
            f"{_sql_string(row['extraction_method'])}, "
            f"TIMESTAMP {_sql_string(extracted_at)}, "
            f"{_sql_string(row['source_system'])}"
            ")"
        )

    cursor.execute(
        "INSERT INTO pdf_extractions "
        "(filename, source_key, bronze_key, extracted_text, char_count, word_count, extraction_method, extracted_at, source_system) "
        "VALUES "
        + ", ".join(values)
    )
    cursor.close()
    conn.close()


def list_new_pdfs(**context):
    s3 = _get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)
        pdf_files = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".pdf")
        ]
    except Exception as exc:
        print(f"Error listing PDFs: {exc}")
        pdf_files = []

    print(f"Found {len(pdf_files)} new PDFs")
    context["ti"].xcom_push(key="pdf_list", value=json.dumps(pdf_files))
    return len(pdf_files)


def process_pdfs(**context):
    import httpx

    ti = context["ti"]
    pdf_list = json.loads(ti.xcom_pull(key="pdf_list", task_ids="list_new_pdfs") or "[]")

    if not pdf_list:
        print("No PDFs to process")
        return

    s3 = _get_s3_client()
    processed = []
    bronze_rows = []
    failed = []

    for pdf_key in pdf_list:
        filename = pdf_key.split("/")[-1]
        print(f"Processing: {filename}")

        try:
            response = s3.get_object(Bucket=SOURCE_BUCKET, Key=pdf_key)
            pdf_content = response["Body"].read()

            tika_response = httpx.put(
                f"{TIKA_URL}/tika",
                content=pdf_content,
                headers={
                    "Content-Type": "application/pdf",
                    "X-Tika-OCRLanguage": "eng",
                },
                timeout=120.0,
            )

            if tika_response.status_code == 200:
                extracted_text = tika_response.text
                extraction_method = "tika"
            else:
                print(f"Tika returned {tika_response.status_code}, retrying with inline image extraction")
                tika_response = httpx.put(
                    f"{TIKA_URL}/tika",
                    content=pdf_content,
                    headers={
                        "Content-Type": "application/pdf",
                        "X-Tika-PDFextractInlineImages": "true",
                        "X-Tika-OCRLanguage": "eng",
                    },
                    timeout=180.0,
                )
                extracted_text = tika_response.text if tika_response.status_code == 200 else ""
                extraction_method = "tika_ocr"

            # ── Tesseract fallback (P3) ──────────────────────────────────────
            # When both Tika passes return empty text, try Tesseract OCR.
            # This handles scanned/image-only PDFs that Tika cannot parse.
            if not extracted_text.strip():
                print(f"Tika returned no text for {filename}; attempting Tesseract OCR fallback")
                extracted_text = _ocr_with_tesseract(pdf_content, filename)
                extraction_method = "tesseract_ocr" if extracted_text.strip() else "failed"
            # ────────────────────────────────────────────────────────────────

            if not extracted_text.strip():
                print(f"WARNING: No text extracted from {filename}")
                failed.append(pdf_key)
                continue

            bronze_key = f"{BRONZE_PREFIX}{datetime.utcnow().strftime('%Y/%m/%d')}/{filename}.json"
            result = {
                "filename": filename,
                "source_key": pdf_key,
                "bronze_key": bronze_key,
                "extracted_text": extracted_text,
                "char_count": len(extracted_text),
                "word_count": len(extracted_text.split()),
                "extraction_method": extraction_method,
                "extracted_at": datetime.utcnow().isoformat(),
                "source_system": "document_store",
            }

            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=bronze_key,
                Body=json.dumps(result, default=str),
                ContentType="application/json",
            )
            s3.copy_object(
                Bucket=SOURCE_BUCKET,
                Key=f"{PROCESSED_PREFIX}{filename}",
                CopySource={"Bucket": SOURCE_BUCKET, "Key": pdf_key},
            )
            s3.delete_object(Bucket=SOURCE_BUCKET, Key=pdf_key)

            processed.append(filename)
            bronze_rows.append(result)
            print(f"Extracted {result['word_count']} words from {filename}")

        except Exception as exc:
            print(f"Error processing {filename}: {exc}")
            failed.append(pdf_key)
            try:
                s3.copy_object(
                    Bucket=SOURCE_BUCKET,
                    Key=f"{DLQ_PREFIX}{filename}",
                    CopySource={"Bucket": SOURCE_BUCKET, "Key": pdf_key},
                )
            except Exception:
                pass

    _write_bronze_rows(bronze_rows)
    ti.xcom_push(key="processed_count", value=len(processed))
    ti.xcom_push(key="failed_count", value=len(failed))
    ti.xcom_push(key="processed_prefix", value=BRONZE_PREFIX)
    print(f"Results: {len(processed)} processed, {len(failed)} failed")


def trigger_milvus_indexer(**context):
    ti = context["ti"]
    processed_count = ti.xcom_pull(key="processed_count", task_ids="process_pdfs")

    if not processed_count or processed_count == 0:
        print("No new documents to index")
        return

    print(f"Triggering Milvus indexer for {processed_count} new documents")
    result = subprocess.run(
        [
            "docker",
            "exec",
            MILVUS_INDEXER_CONTAINER,
            "python",
            "/app/utils/index_documents.py",
            "--bucket",
            BRONZE_BUCKET,
            "--collection",
            MILVUS_COLLECTION,
            "--prefix",
            BRONZE_PREFIX,
            "--tika-url",
            TIKA_URL,
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Milvus indexer failed with exit code {result.returncode}")
    print(f"Successfully indexed {processed_count} documents into Milvus")


t_list_pdfs = PythonOperator(
    task_id="list_new_pdfs",
    python_callable=list_new_pdfs,
    dag=dag,
)

t_process = PythonOperator(
    task_id="process_pdfs",
    python_callable=process_pdfs,
    dag=dag,
)

t_index = PythonOperator(
    task_id="trigger_milvus_indexer",
    python_callable=trigger_milvus_indexer,
    dag=dag,
)

t_notify = BashOperator(
    task_id="notify_completion",
    bash_command=(
        'echo "PDF Pipeline completed at $(date). '
        'Processed: {{ ti.xcom_pull(key=\'processed_count\', task_ids=\'process_pdfs\') }} files, '
        'Failed: {{ ti.xcom_pull(key=\'failed_count\', task_ids=\'process_pdfs\') }} files"'
    ),
    dag=dag,
)

t_list_pdfs >> t_process >> t_index >> t_notify
