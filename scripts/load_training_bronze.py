from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Iterable

import trino


TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "admin"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "bronze"
TABLE_NAME = "tms_training_completions"


def quote(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (int, float)):
        return str(value)
    raise TypeError(f"Unsupported literal type: {type(value)!r}")


def read_records(lines: Iterable[str]) -> list[dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        record_id = payload.get("record_id")
        if not record_id:
            continue
        records[record_id] = {
            "record_id": record_id,
            "employee_id": payload.get("employee_id"),
            "employee_name": payload.get("employee_name"),
            "department": payload.get("department"),
            "training_name": payload.get("training_name"),
            "training_category": payload.get("training_category"),
            "scheduled_date": payload.get("scheduled_date"),
            "completion_date": payload.get("completion_date"),
            "score": payload.get("score"),
            "status": payload.get("status"),
            "trainer_id": payload.get("trainer_id"),
            "training_mode": payload.get("training_mode"),
            "validity_months": payload.get("validity_months"),
            "_source": payload.get("_source", "TMS"),
            "_ingested_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
    return list(records.values())


def batch(items: list[dict[str, object]], size: int) -> Iterable[list[dict[str, object]]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def main() -> int:
    records = read_records(sys.stdin)
    if not records:
        print("No training records found on stdin.", file=sys.stderr)
        return 1

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
    cur.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TABLE_NAME}")
    cur.execute(
        f"""
        CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TABLE_NAME} (
            record_id VARCHAR,
            employee_id VARCHAR,
            employee_name VARCHAR,
            department VARCHAR,
            training_name VARCHAR,
            training_category VARCHAR,
            scheduled_date DATE,
            completion_date DATE,
            score INTEGER,
            status VARCHAR,
            trainer_id VARCHAR,
            training_mode VARCHAR,
            validity_months INTEGER,
            _source VARCHAR,
            _ingested_at TIMESTAMP(6)
        )
        """
    )

    for chunk in batch(records, 200):
        values = []
        for row in chunk:
            values.append(
                "("
                + ", ".join(
                    [
                        quote(row["record_id"]),
                        quote(row["employee_id"]),
                        quote(row["employee_name"]),
                        quote(row["department"]),
                        quote(row["training_name"]),
                        quote(row["training_category"]),
                        f"DATE {quote(row['scheduled_date'])}" if row["scheduled_date"] else "NULL",
                        f"DATE {quote(row['completion_date'])}" if row["completion_date"] else "NULL",
                        quote(row["score"]),
                        quote(row["status"]),
                        quote(row["trainer_id"]),
                        quote(row["training_mode"]),
                        quote(row["validity_months"]),
                        quote(row["_source"]),
                        f"TIMESTAMP {quote(row['_ingested_at'])}",
                    ]
                )
                + ")"
            )
        sql = (
            f"INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{TABLE_NAME} VALUES "
            + ", ".join(values)
        )
        cur.execute(sql)

    cur.execute(f"SELECT COUNT(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TABLE_NAME}")
    count = cur.fetchone()[0]
    print(f"Loaded {count} training rows into {TRINO_CATALOG}.{TRINO_SCHEMA}.{TABLE_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
