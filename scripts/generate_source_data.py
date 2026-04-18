from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from uuid import uuid4


random.seed(42)

ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT_DIR / "data" / "source"

PRODUCTS = [
    ("API-100", "Paracetamol 500mg"),
    ("API-200", "Amoxicillin 250mg"),
    ("API-300", "Cetirizine 10mg"),
    ("API-400", "Metformin 500mg"),
    ("API-500", "Omeprazole 20mg"),
]
PARAMETER_SPECS = {
    "temperature": ("C", (18.0, 34.5)),
    "pressure": ("bar", (0.8, 9.2)),
    "rpm": ("rpm", (450.0, 1800.0)),
    "viscosity": ("cP", (80.0, 400.0)),
    "ph_level": ("pH", (6.6, 8.2)),
}
SHIFTS = ["A", "B", "C"]
MES_STATUSES = ["PASS", "FAIL", "WARNING"]
ORDER_STATUSES = ["PLANNED", "IN_PROGRESS", "COMPLETE", "DELAYED"]
DEVIATION_TYPES = [
    "TEMPERATURE_EXCURSION",
    "CONTAMINATION",
    "EQUIPMENT_FAILURE",
    "DOCUMENTATION_ERROR",
]
DEVIATION_SEVERITIES = ["CRITICAL", "MAJOR", "MINOR"]
DEVIATION_STATUSES = ["OPEN", "IN_REVIEW", "CLOSED"]
PLANTS = ["PLANT01", "PLANT02", "PLANT03"]
STORAGE_LOCATIONS = ["RM01", "FG01", "BLK1", "QC02"]
UOMS = ["KG", "L", "EA"]
DEPARTMENTS = ["Manufacturing", "Quality", "Engineering", "Validation"]
DOC_TYPES = ["SOP", "CAPA", "BATCH_RECORD", "DEVIATION_REPORT"]
DOC_AUTHORS = ["A. Sharma", "P. Iyer", "N. Rao", "S. Menon", "R. Gupta"]
OPERATORS = [f"OP{idx:03d}" for idx in range(1, 31)]
LINES = [f"LINE-{idx}" for idx in range(1, 9)]
MACHINES = [f"MCH-{idx:03d}" for idx in range(1, 16)]
BATCH_IDS = [f"BATCH-{idx:05d}" for idx in range(1001, 1301)]


@dataclass(frozen=True)
class DatasetSpec:
    filename: str
    headers: list[str]
    rows: list[dict[str, object]]


def _choice_product() -> tuple[str, str]:
    return random.choice(PRODUCTS)


def _random_timestamp_within(hours: int) -> datetime:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    offset_seconds = random.randint(0, hours * 3600)
    return now - timedelta(seconds=offset_seconds)


def build_mes_events() -> list[dict[str, object]]:
    rows = []
    for idx in range(500):
        product_code, _ = _choice_product()
        parameter_name = random.choice(list(PARAMETER_SPECS))
        unit, bounds = PARAMETER_SPECS[parameter_name]
        status = random.choices(MES_STATUSES, weights=[0.82, 0.08, 0.10])[0]
        value = round(random.uniform(*bounds), 2)
        if status == "FAIL":
            value = round(value * random.choice([0.45, 1.55]), 2)
        elif status == "WARNING":
            value = round(value * random.choice([0.85, 1.15]), 2)

        rows.append(
            {
                "event_id": f"EVT-{idx + 1:06d}",
                "machine_id": random.choice(MACHINES),
                "batch_id": random.choice(BATCH_IDS),
                "product_code": product_code,
                "parameter_name": parameter_name,
                "parameter_value": value,
                "unit": unit,
                "operator_id": random.choice(OPERATORS),
                "shift": random.choice(SHIFTS),
                "event_ts": _random_timestamp_within(24).strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
            }
        )
    return rows


def build_iqms_orders() -> list[dict[str, object]]:
    rows = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for idx in range(200):
        product_code, _ = _choice_product()
        planned_start = now - timedelta(hours=random.randint(4, 96))
        actual_start = planned_start + timedelta(minutes=random.randint(0, 180))
        actual_end = actual_start + timedelta(hours=random.randint(2, 18))
        status = random.choices(ORDER_STATUSES, weights=[0.1, 0.2, 0.6, 0.1])[0]
        if status == "PLANNED":
            actual_start = planned_start
            actual_end = actual_start + timedelta(hours=random.randint(3, 8))

        rows.append(
            {
                "order_id": f"ORD-{idx + 1:05d}",
                "product_code": product_code,
                "batch_id": random.choice(BATCH_IDS),
                "quantity": random.randint(800, 10000),
                "uom": random.choice(UOMS),
                "planned_start": planned_start.strftime("%Y-%m-%d %H:%M:%S"),
                "actual_start": actual_start.strftime("%Y-%m-%d %H:%M:%S"),
                "actual_end": actual_end.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
                "line_id": random.choice(LINES),
            }
        )
    return rows


def build_trackwise_deviations() -> list[dict[str, object]]:
    rows = []
    for idx in range(100):
        product_code, product_name = _choice_product()
        reported_ts = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=random.randint(1, 240))
        status = random.choices(DEVIATION_STATUSES, weights=[0.2, 0.25, 0.55])[0]
        resolution_ts = ""
        if status == "CLOSED":
            resolution_ts = (reported_ts + timedelta(hours=random.randint(4, 96))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        rows.append(
            {
                "deviation_id": f"DEV-{idx + 1:05d}",
                "batch_id": random.choice(BATCH_IDS),
                "product_code": product_code,
                "deviation_type": random.choice(DEVIATION_TYPES),
                "severity": random.choices(DEVIATION_SEVERITIES, weights=[0.15, 0.35, 0.5])[0],
                "description": f"{product_name} deviation investigation for lot review {idx + 1}",
                "reported_by": random.choice(OPERATORS),
                "reported_ts": reported_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
                "resolution_ts": resolution_ts,
            }
        )
    return rows


def build_sap_orders() -> list[dict[str, object]]:
    rows = []
    for idx in range(150):
        product_code, _ = _choice_product()
        planned_qty = random.randint(1000, 12000)
        actual_qty = max(0, planned_qty + random.randint(-500, 650))
        rows.append(
            {
                "po_number": f"PO-{500000 + idx}",
                "material_code": product_code.replace("API", "MAT"),
                "plant": random.choice(PLANTS),
                "storage_location": random.choice(STORAGE_LOCATIONS),
                "planned_qty": planned_qty,
                "actual_qty": actual_qty,
                "uom": random.choice(UOMS),
                "posting_date": (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=random.randint(0, 30))).strftime(
                    "%Y-%m-%d"
                ),
                "cost_center": f"CC-{random.randint(100, 999)}",
                "total_cost": round(actual_qty * random.uniform(8.5, 22.0), 2),
                "currency": "USD",
            }
        )
    return rows


def build_sop_documents() -> list[dict[str, object]]:
    rows = []
    for idx in range(50):
        doc_type = random.choice(DOC_TYPES)
        department = random.choice(DEPARTMENTS)
        rows.append(
            {
                "doc_id": f"DOC-{idx + 1:04d}",
                "doc_type": doc_type,
                "title": f"{department} {doc_type.replace('_', ' ').title()} {idx + 1}",
                "version": f"{random.randint(1, 5)}.{random.randint(0, 9)}",
                "effective_date": (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=random.randint(0, 720))).strftime(
                    "%Y-%m-%d"
                ),
                "author": random.choice(DOC_AUTHORS),
                "department": department,
                "file_path": f"/docs/{department.lower()}/{doc_type.lower()}/{idx + 1:04d}.pdf",
                "page_count": random.randint(4, 120),
            }
        )
    return rows


def write_csv(path: Path, headers: Iterable[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    datasets = [
        DatasetSpec(
            filename="mes_events.csv",
            headers=[
                "event_id",
                "machine_id",
                "batch_id",
                "product_code",
                "parameter_name",
                "parameter_value",
                "unit",
                "operator_id",
                "shift",
                "event_ts",
                "status",
            ],
            rows=build_mes_events(),
        ),
        DatasetSpec(
            filename="iqms_orders.csv",
            headers=[
                "order_id",
                "product_code",
                "batch_id",
                "quantity",
                "uom",
                "planned_start",
                "actual_start",
                "actual_end",
                "status",
                "line_id",
            ],
            rows=build_iqms_orders(),
        ),
        DatasetSpec(
            filename="trackwise_deviations.csv",
            headers=[
                "deviation_id",
                "batch_id",
                "product_code",
                "deviation_type",
                "severity",
                "description",
                "reported_by",
                "reported_ts",
                "status",
                "resolution_ts",
            ],
            rows=build_trackwise_deviations(),
        ),
        DatasetSpec(
            filename="sap_ecc_orders.csv",
            headers=[
                "po_number",
                "material_code",
                "plant",
                "storage_location",
                "planned_qty",
                "actual_qty",
                "uom",
                "posting_date",
                "cost_center",
                "total_cost",
                "currency",
            ],
            rows=build_sap_orders(),
        ),
        DatasetSpec(
            filename="sop_documents.csv",
            headers=[
                "doc_id",
                "doc_type",
                "title",
                "version",
                "effective_date",
                "author",
                "department",
                "file_path",
                "page_count",
            ],
            rows=build_sop_documents(),
        ),
    ]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for dataset in datasets:
      output_path = OUTPUT_DIR / dataset.filename
      write_csv(output_path, dataset.headers, dataset.rows)
      print(f"{output_path.relative_to(ROOT_DIR)}: {len(dataset.rows)} rows")


if __name__ == "__main__":
    main()
