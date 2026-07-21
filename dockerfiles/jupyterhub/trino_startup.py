# Auto-loaded by IPython at kernel startup.
# Pre-configures Trino connection so analysts can query immediately.

import os

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs-s3:8333")

print(f"[Lakehouse] Trino: {TRINO_HOST}:{TRINO_PORT} | S3: {S3_ENDPOINT}")
print("[Lakehouse] Quickstart:")
print("  import trino")
print("  conn = trino.dbapi.connect(host=os.environ['TRINO_HOST'], port=8080, user='admin', catalog='iceberg')")
print("  cur = conn.cursor()")
print("  cur.execute(\"SELECT * FROM iceberg.gold.gold_oee_dashboard LIMIT 10\")")
print("  rows = cur.fetchall()")
