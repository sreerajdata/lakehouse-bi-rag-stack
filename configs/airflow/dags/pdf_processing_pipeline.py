"""
Downloads PDFs from SeaweedFS, extracts text via Tika/OCR, stores in bronze,
and triggers Milvus indexing for RAG search.
"""

from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

S3_ENDPOINT = os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333")
S3_ACCESS_KEY = os.getenv("SEAWEEDFS_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("SEAWEEDFS_SECRET_KEY", "admin123")
TIKA_URL = os.getenv("TIKA_URL", "http://tika:9998")

SOURCE_BUCKET = "lakehouse-docs"
SOURCE_PREFIX = "incoming/"
PROCESSED_PREFIX = "processed/"
BRONZE_BUCKET = "lakehouse-bronze"
BRONZE_PREFIX = "pdf_extractions/"
DLQ_PREFIX = "dlq/pdf/"

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
    description="PDF Processing Pipeline: SeaweedFS → Tika → Bronze → Milvus",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pdf", "document", "bronze", "rag"],
)


def _get_s3_client():
    """Create boto3 S3 client pointing to SeaweedFS."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def list_new_pdfs(**context):
    """List new PDFs in the incoming folder of SeaweedFS."""
    s3 = _get_s3_client()
    try:
        response = s3.list_objects_v2(
            Bucket=SOURCE_BUCKET,
            Prefix=SOURCE_PREFIX,
        )
        pdf_files = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".pdf")
        ]
    except Exception as e:
        print(f"Error listing PDFs: {e}")
        pdf_files = []

    print(f"Found {len(pdf_files)} new PDFs")
    context["ti"].xcom_push(key="pdf_list", value=json.dumps(pdf_files))
    return len(pdf_files)


def process_pdfs(**context):
    """Download PDFs, extract text via Tika, store results in bronze."""
    import httpx

    ti = context["ti"]
    pdf_list = json.loads(ti.xcom_pull(key="pdf_list", task_ids="list_new_pdfs"))

    if not pdf_list:
        print("No PDFs to process")
        return

    s3 = _get_s3_client()
    processed = []
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
            else:
                print(f"  Tika returned {tika_response.status_code}, trying OCR...")
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

            if not extracted_text.strip():
                print(f"  WARNING: No text extracted from {filename}")
                failed.append(pdf_key)
                continue

            result = {
                "filename": filename,
                "source_key": pdf_key,
                "extracted_text": extracted_text,
                "char_count": len(extracted_text),
                "word_count": len(extracted_text.split()),
                "extraction_method": "tika",
                "extracted_at": datetime.utcnow().isoformat(),
                "source_system": "document_store",
            }

            bronze_key = f"{BRONZE_PREFIX}{datetime.utcnow().strftime('%Y/%m/%d')}/{filename}.json"
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
            print(f"  ✅ Extracted {result['word_count']} words from {filename}")

        except Exception as e:
            print(f"  ❌ Error processing {filename}: {e}")
            failed.append(pdf_key)

            try:
                s3.copy_object(
                    Bucket=SOURCE_BUCKET,
                    Key=f"{DLQ_PREFIX}{filename}",
                    CopySource={"Bucket": SOURCE_BUCKET, "Key": pdf_key},
                )
            except Exception:
                pass

    ti.xcom_push(key="processed_count", value=len(processed))
    ti.xcom_push(key="failed_count", value=len(failed))
    print(f"\nResults: {len(processed)} processed, {len(failed)} failed")


def trigger_milvus_indexer(**context):
    """Trigger Milvus document indexing for newly processed PDFs."""
    ti = context["ti"]
    processed_count = ti.xcom_pull(key="processed_count", task_ids="process_pdfs")

    if not processed_count or processed_count == 0:
        print("No new documents to index")
        return

    print(f"Triggering Milvus indexer for {processed_count} new documents...")

    try:
        import sys
        sys.path.insert(0, "/app/utils")
        from index_documents import index_documents

        index_documents(
            bucket=BRONZE_BUCKET,
            collection="enterprise_documents",
            prefix=BRONZE_PREFIX,
        )
        print(f"✅ Successfully indexed {processed_count} documents into Milvus")
    except ImportError:
        import httpx
        try:
            response = httpx.post(
                "http://langchain-app:8501/api/index",
                json={"bucket": BRONZE_BUCKET, "prefix": BRONZE_PREFIX},
                timeout=300.0,
            )
            print(f"Indexer response: {response.status_code}")
        except Exception as e:
            print(f"⚠️ Could not trigger indexer: {e}")


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
