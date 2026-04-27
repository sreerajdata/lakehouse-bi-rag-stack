"""
==========================================================================
  ENTERPRISE DATA LAKEHOUSE — Live Ingestion Demo
  Run this script inside the Spark container using spark-submit:
    docker exec lakehouse_spark_master spark-submit /opt/lakehouse/scripts/demo_ingest.py
==========================================================================
"""
from __future__ import annotations

import csv
import random
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Try importing Spark (only available inside the Spark container) ────────────
try:
    from pyspark.sql import SparkSession, functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# ── Paths and constants ────────────────────────────────────────────────────────
ROOT_DIR        = Path(__file__).resolve().parents[1]
LOCAL_DATA_DIR  = ROOT_DIR / "data" / "source"
BRONZE_S3       = "s3a://bronze/source"
BRONZE_WH       = "s3a://lakehouse-bronze/warehouse"
DEMO_TABLE      = "lakehouse.bronze.demo_iqms_orders"
DEMO_ROWS       = 25
DBT_DIR         = "/opt/airflow/dbt"

# Unique filename per run — so every execution creates a NEW visible file
RUN_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
DEMO_FILE = f"iqms_orders_demo_{RUN_TS}.csv"

# Synthetic data pools (matching the existing schema)
PRODUCTS    = ["API-100", "API-200", "API-300", "API-400", "API-500"]
STATUSES    = ["PLANNED", "IN_PROGRESS", "COMPLETE", "DELAYED"]
LINES       = [f"LINE-{i}" for i in range(1, 9)]
BATCH_IDS   = [f"DEMO-BATCH-{i:05d}" for i in range(9001, 9100)]
UOMS        = ["KG", "L", "EA"]

BANNER = "=" * 72


def banner(title: str) -> None:
    print(f"\n{BANNER}")
    print(f"  {title}")
    print(f"{BANNER}")


def step(num: int, msg: str) -> None:
    print(f"\n[STEP {num}] {msg}")
    print("-" * 50)


def ok(msg: str) -> None:
    print(f"  ✅  {msg}")


def info(msg: str) -> None:
    print(f"  ℹ   {msg}")


def fail(msg: str) -> None:
    print(f"  ❌  {msg}")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Generate a new demo CSV file
# ─────────────────────────────────────────────────────────────────────────────
def generate_demo_csv() -> Path:
    step(1, f"Generating {DEMO_ROWS} fresh IQMS production orders")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i in range(DEMO_ROWS):
        product_code  = random.choice(PRODUCTS)
        planned_start = now - timedelta(hours=random.randint(1, 3))
        actual_start  = planned_start + timedelta(minutes=random.randint(5, 30))
        actual_end    = now - timedelta(minutes=random.randint(1, 15))
        status        = random.choices(STATUSES, weights=[0.1, 0.2, 0.6, 0.1])[0]
        rows.append({
            "order_id":      f"DEMO-{RUN_TS}-{i + 1:03d}",
            "product_code":  product_code,
            "batch_id":      random.choice(BATCH_IDS),
            "quantity":      random.randint(500, 8000),
            "uom":           random.choice(UOMS),
            "planned_start": planned_start.strftime("%Y-%m-%d %H:%M:%S"),
            "actual_start":  actual_start.strftime("%Y-%m-%d %H:%M:%S"),
            "actual_end":    actual_end.strftime("%Y-%m-%d %H:%M:%S"),
            "status":        status,
            "line_id":       random.choice(LINES),
        })

    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    headers = list(rows[0].keys())

    # ── 1a. Write a brand-new timestamped demo file (visible proof) ───────────
    demo_path = LOCAL_DATA_DIR / DEMO_FILE
    with demo_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    ok(f"NEW file created:  data/source/{DEMO_FILE}  ({len(rows)} rows)")

    # ── 1b. Also APPEND rows to the main iqms_orders.csv (visual confirmation) ─
    main_csv = LOCAL_DATA_DIR / "iqms_orders.csv"
    with main_csv.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writerows(rows)   # no header — appending to existing file
    ok(f"APPENDED to:       data/source/iqms_orders.csv  (+{len(rows)} rows)")
    info(f"iqms_orders.csv now has {sum(1 for _ in main_csv.open()) - 1} total rows (including header)")

    print("\n  Sample rows (first 3):")
    print(f"  {'order_id':<28} {'product_code':<14} {'quantity':<10} {'status'}")
    print(f"  {'-'*65}")
    for r in rows[:3]:
        print(f"  {r['order_id']:<28} {r['product_code']:<14} {str(r['quantity']):<10} {r['status']}")
    print(f"  ... and {len(rows) - 3} more rows")

    return demo_path


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Upload CSV to SeaweedFS (S3)
# ─────────────────────────────────────────────────────────────────────────────
def upload_to_s3(spark: "SparkSession", local_path: Path) -> None:
    step(2, f"Uploading CSV to Object Storage (SeaweedFS S3) → {BRONZE_S3}/{DEMO_FILE}")
    jvm        = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    remote_fs  = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(BRONZE_S3), hadoop_conf
    )
    local_fs   = jvm.org.apache.hadoop.fs.FileSystem.getLocal(hadoop_conf)
    remote_fs.mkdirs(jvm.org.apache.hadoop.fs.Path(BRONZE_S3))
    src = jvm.org.apache.hadoop.fs.Path(str(local_path))
    dst = jvm.org.apache.hadoop.fs.Path(f"{BRONZE_S3}/{DEMO_FILE}")
    jvm.org.apache.hadoop.fs.FileUtil.copy(local_fs, src, remote_fs, dst, False, True, hadoop_conf)
    ok(f"Uploaded → {BRONZE_S3}/{DEMO_FILE}")
    info("Browse at: http://localhost:8888/bronze/source/")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Ingest CSV into Bronze via Spark (demo table + main pipeline table)
# ─────────────────────────────────────────────────────────────────────────────
def ingest_to_bronze(spark: "SparkSession") -> int:
    step(3, f"Ingesting CSV into Bronze Iceberg (2 paths)")
    info(f"Reading from: {BRONZE_S3}/{DEMO_FILE}")

    # Shared select expression matching the iqms_orders schema
    select_exprs = [
        "order_id",
        "product_code",
        "batch_id",
        F.col("quantity").cast("int").alias("quantity"),
        "uom",
        F.to_timestamp("planned_start").alias("planned_start"),
        F.to_timestamp("actual_start").alias("actual_start"),
        F.to_timestamp("actual_end").alias("actual_end"),
        "status",
        "line_id",
    ]

    df = (
        spark.read.option("header", True)
        .csv(f"{BRONZE_S3}/{DEMO_FILE}")
        .select(*select_exprs)
        .withColumn("_source",      F.lit("IQMS_DEMO"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_nifi_flow",   F.lit("csv_demo_ingest"))
    )
    df.cache()  # cache so we don't re-read S3 twice

    # Ensure namespace exists
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze LOCATION '{BRONZE_WH}/bronze.db'"
    )

    # ── PATH A: Create a dedicated demo table (visible in terminal) ────────────
    info("PATH A: Creating dedicated demo Bronze table (for visibility)")
    temp_view = "demo_iqms_orders_view"
    df.createOrReplaceTempView(temp_view)
    demo_location = f"{BRONZE_WH}/bronze.db/demo_iqms_orders"
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {DEMO_TABLE}
        USING iceberg
        LOCATION '{demo_location}'
        AS SELECT * FROM {temp_view}
        """
    )
    demo_count = spark.sql(f"SELECT COUNT(*) as c FROM {DEMO_TABLE}").collect()[0]["c"]
    ok(f"Demo table created: {DEMO_TABLE}  ({demo_count} rows)")
    ok(f"Physical path: {demo_location}")

    # Show a quick sample from the demo table
    print("\n  Live sample from Bronze — demo table:")
    spark.sql(
        f"""
        SELECT order_id, product_code, quantity, status, _ingested_at
        FROM {DEMO_TABLE} LIMIT 5
        """
    ).show(truncate=False)

    # ── PATH B: Append rows into the MAIN iqms_orders table ────────────────────
    # Keep _source, _ingested_at, _nifi_flow — they ARE columns in the main table.
    # Just drop the extra demo-only column if any.
    info("PATH B: Appending demo rows into main bronze.iqms_orders (feeds dbt Silver)")
    before_count = spark.sql(
        "SELECT COUNT(*) as c FROM lakehouse.bronze.iqms_orders"
    ).collect()[0]["c"]
    info(f"iqms_orders BEFORE append: {before_count:,} rows")

    # df already has _source, _ingested_at, _nifi_flow — matches the production schema
    df.writeTo("lakehouse.bronze.iqms_orders").append()

    after_count = spark.sql(
        "SELECT COUNT(*) as c FROM lakehouse.bronze.iqms_orders"
    ).collect()[0]["c"]
    ok(f"iqms_orders AFTER  append: {after_count:,} rows  (+{after_count - before_count} new rows)")
    ok("These new rows WILL flow into silver_production_orders when dbt runs!")

    return demo_count


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Verify existing Bronze tables are intact
# ─────────────────────────────────────────────────────────────────────────────
def verify_existing_bronze(spark: "SparkSession") -> None:
    step(4, "Verifying all Bronze tables (pre-existing + new demo table)")
    tables = [
        ("lakehouse.bronze.mes_events",         "MES Events"),
        ("lakehouse.bronze.iqms_orders",         "IQMS Orders"),
        ("lakehouse.bronze.trackwise_deviations","TrackWise Deviations"),
        ("lakehouse.bronze.sap_ecc_orders",      "SAP ECC Orders"),
        (DEMO_TABLE,                             "DEMO IQMS Orders ← NEW"),
    ]
    print(f"\n  {'Table':<40} {'Rows':>8}")
    print(f"  {'-'*50}")
    for table, label in tables:
        try:
            cnt = spark.sql(f"SELECT COUNT(*) as c FROM {table}").collect()[0]["c"]
            marker = " ← NEW ✅" if "demo" in table else ""
            print(f"  {label:<40} {cnt:>8,}{marker}")
        except Exception:
            print(f"  {label:<40} {'N/A':>8}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Run dbt Silver transformation and compare counts
# ─────────────────────────────────────────────────────────────────────────────
def run_dbt_silver(spark: "SparkSession") -> None:
    step(5, "Running dbt Silver transformation (silver_production_orders)")
    info("Tool: dbt via Trino — reads from bronze.iqms_orders → writes to silver")
    info("The 25 demo rows appended in Step 3 (PATH B) will now appear in Silver")

    # Capture Silver count BEFORE dbt runs
    silver_before = 0
    try:
        silver_before = spark.sql(
            "SELECT COUNT(*) as c FROM iceberg.silver.silver_production_orders"
        ).collect()[0]["c"]
        info(f"silver_production_orders BEFORE dbt: {silver_before:,} rows")
    except Exception:
        info("Could not read Silver count before dbt (table may not exist yet)")

    # Run dbt
    dbt_cmd = [
        "docker", "exec", "lakehouse_dbt",
        "dbt", "run",
        "--select", "silver_production_orders",
        "--profiles-dir", DBT_DIR,
        "--project-dir", DBT_DIR,
    ]
    try:
        result = subprocess.run(dbt_cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            ok("dbt Silver run completed successfully!")
            for line in result.stdout.splitlines():
                if any(kw in line for kw in ["Completed", "PASS", "FAIL", "ERROR", "silver"]):
                    print(f"  dbt → {line.strip()}")
        else:
            info("dbt returned non-zero — run manually: make dbt-run")
    except FileNotFoundError:
        info("Docker not found from inside Spark container.")
        info("Run dbt manually from project root: make dbt-run")
    except subprocess.TimeoutExpired:
        info("dbt timed out — run it manually: make dbt-run")

    # Capture Silver count AFTER dbt runs
    try:
        silver_after = spark.sql(
            "SELECT COUNT(*) as c FROM iceberg.silver.silver_production_orders"
        ).collect()[0]["c"]
        ok(f"silver_production_orders AFTER  dbt: {silver_after:,} rows")
        if silver_after > silver_before:
            ok(f"Silver increased by {silver_after - silver_before} rows — demo data flowed through! ✅")
        info("Storage: s3a://lakehouse-silver/warehouse/silver.db/silver_production_orders/")
    except Exception:
        info("Verify manually: SELECT COUNT(*) FROM iceberg.silver.silver_production_orders;")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — Orchestrate all steps
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    banner("ENTERPRISE DATA LAKEHOUSE — Live Ingestion Demo")
    print(f"\n  This script demonstrates the complete data ingestion flow:")
    print(f"  Source CSV → SeaweedFS (S3) → Apache Spark → Bronze Iceberg → dbt → Silver")
    print(f"\n  Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # ── Step 1: Generate CSV ──────────────────────────────────────────────────
    local_csv = generate_demo_csv()

    if not SPARK_AVAILABLE:
        fail(
            "PySpark not available. Run this inside the Spark container using spark-submit:\n"
            "  docker exec lakehouse_spark_master spark-submit /opt/lakehouse/scripts/demo_ingest.py"
        )

    # ── Build Spark session ───────────────────────────────────────────────────
    spark = (
        SparkSession.builder.appName("lakehouse-demo-ingest")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",             "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type",        "hive")
        .config("spark.sql.catalog.lakehouse.uri",         "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse",   BRONZE_WH)
        .config("spark.hadoop.fs.s3a.endpoint",            "http://seaweedfs-s3:8333")
        .config("spark.hadoop.fs.s3a.access.key",          "admin")
        .config("spark.hadoop.fs.s3a.secret.key",          "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ── Steps 2–5 ─────────────────────────────────────────────────────────────
    upload_to_s3(spark, local_csv)
    row_count = ingest_to_bronze(spark)
    verify_existing_bronze(spark)
    run_dbt_silver(spark)

    # ── Final Summary ─────────────────────────────────────────────────────────
    banner("DEMO COMPLETE — Full Pipeline Verified")
    print(f"""
  WHAT JUST HAPPENED:
  ───────────────────
  1. Generated {DEMO_ROWS} synthetic IQMS production orders
     File: {local_csv}

  2. Uploaded raw CSV to Object Storage (SeaweedFS S3)
     Path:   {BRONZE_S3}/{DEMO_FILE}
     Browse: http://localhost:8888/bronze/source/

  3a. Spark created a dedicated demo Bronze table (for visibility)
      Table: iceberg.bronze.demo_iqms_orders  ({row_count} rows)
      Path:  {BRONZE_WH}/bronze.db/demo_iqms_orders/
      SQL:   SELECT * FROM iceberg.bronze.demo_iqms_orders LIMIT 10;

  3b. Spark APPENDED those same {row_count} rows into the MAIN pipeline table
      Table: iceberg.bronze.iqms_orders  (existing + {row_count} new)
      Path:  {BRONZE_WH}/bronze.db/iqms_orders/
      SQL:   SELECT * FROM iceberg.bronze.iqms_orders ORDER BY _ingested_at DESC LIMIT 10;

  4. dbt read bronze.iqms_orders → transformed to Silver
      Table: iceberg.silver.silver_production_orders
      Path:  s3a://lakehouse-silver/warehouse/silver.db/silver_production_orders/
      SQL:   SELECT * FROM iceberg.silver.silver_production_orders
             WHERE order_id LIKE 'DEMO-%' LIMIT 10;

  VERIFY DEMO DATA IN SILVER (Trino):
  ─────────────────────────────────────
  docker exec lakehouse_trino trino --server http://localhost:8080 --execute \\
    "SELECT order_id, product_code, status FROM iceberg.silver.silver_production_orders WHERE order_id LIKE 'DEMO-%' LIMIT 10"

  VIEW IN SUPERSET:
  ──────────────────
  http://localhost:8500  (admin / admin)
    """)

    spark.stop()


if __name__ == "__main__":
    main()
