"""
TPL Data Lakehouse - Synthetic Data Generator
Simulates: MES, IQMS, Historian/L2, Trackwise, SAP ECC, TMS
Outputs to: Kafka topics (streaming) + PostgreSQL (batch)
"""

import json
import os
import time
import random
import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import threading

from faker import Faker
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("synthetic-datagen")
fake = Faker()

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_SERVERS = "kafka:9092"
PG_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "user": "admin",
    "password": "admin123",
    "dbname": "lakehouse_meta"
}

BATCH_SIZE = int(os.getenv("SYNTHETIC_BATCH_SIZE", 100))
STREAM_INTERVAL_MS = int(os.getenv("SYNTHETIC_STREAM_INTERVAL_MS", 500))

# ── Reference Data ────────────────────────────────────────────────────────────
PRODUCTS = ["PROD-API-001", "PROD-TAB-002", "PROD-CAP-003", "PROD-INJ-004", "PROD-SYR-005"]
MACHINES = [f"MCH-{i:03d}" for i in range(1, 21)]
OPERATORS = [f"OPR-{i:03d}" for i in range(1, 51)]
SHIFTS = ["A", "B", "C"]
PLANTS = ["Dahej-P1", "Dahej-P2"]
DEPARTMENTS = ["Manufacturing", "QC", "QA", "Packaging", "Warehouse", "Engineering"]
DEFECT_CODES = ["DEF-001", "DEF-002", "DEF-003", "DEF-004", "DEF-005"]
SAP_VENDORS = [f"VEND-{i:04d}" for i in range(1, 30)]
SAP_CUSTOMERS = [f"CUST-{i:04d}" for i in range(1, 50)]
MATERIALS = [f"MAT-{i:05d}" for i in range(1000, 1200)]
CAPA_CODES = ["CAPA-HUM", "CAPA-MAC", "CAPA-MTH", "CAPA-ENV", "CAPA-MAT"]


# ── Kafka Producer ─────────────────────────────────────────────────────────────
def get_kafka_producer():
    retries = 0
    while retries < 10:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3
            )
            logger.info("Kafka producer connected.")
            return producer
        except Exception as e:
            logger.warning(f"Kafka not ready ({e}), retrying in 5s... [{retries}/10]")
            time.sleep(5)
            retries += 1
    raise RuntimeError("Could not connect to Kafka after 10 retries")


# ── MES - Manufacturing Execution System ──────────────────────────────────────
class MESGenerator:
    TOPIC = "mes.production_orders"
    MACHINE_STATUS_TOPIC = "mes.machine_status"
    OEE_TOPIC = "mes.oee_metrics"

    def production_order(self) -> Dict[str, Any]:
        start = fake.date_time_between(start_date="-30d", end_date="now")
        return {
            "order_id": f"PO-{uuid.uuid4().hex[:8].upper()}",
            "product_code": random.choice(PRODUCTS),
            "batch_number": f"BATCH-{fake.numerify('######')}",
            "machine_id": random.choice(MACHINES),
            "operator_id": random.choice(OPERATORS),
            "shift": random.choice(SHIFTS),
            "plant": random.choice(PLANTS),
            "planned_qty": random.randint(500, 5000),
            "actual_qty": random.randint(450, 5000),
            "rejected_qty": random.randint(0, 50),
            "start_time": start.isoformat(),
            "end_time": (start + timedelta(hours=random.randint(2, 8))).isoformat(),
            "status": random.choice(["COMPLETED", "IN_PROGRESS", "ON_HOLD", "COMPLETED"]),
            "scrap_percentage": round(random.uniform(0, 3), 2),
            "_source": "MES",
            "_ingested_at": datetime.utcnow().isoformat(),
            "_event_type": "production_order"
        }

    def machine_status(self) -> Dict[str, Any]:
        return {
            "machine_id": random.choice(MACHINES),
            "status": random.choices(
                ["RUNNING", "IDLE", "MAINTENANCE", "FAULT"],
                weights=[70, 15, 10, 5]
            )[0],
            "temperature_c": round(random.uniform(18, 85), 2),
            "vibration_hz": round(random.uniform(0, 5), 3),
            "speed_rpm": random.randint(0, 3000),
            "cycle_time_s": round(random.uniform(5, 60), 2),
            "timestamp": datetime.utcnow().isoformat(),
            "_source": "MES",
            "_event_type": "machine_status"
        }

    def oee_metric(self) -> Dict[str, Any]:
        availability = round(random.uniform(0.75, 0.99), 4)
        performance = round(random.uniform(0.80, 0.99), 4)
        quality = round(random.uniform(0.95, 0.999), 4)
        return {
            "metric_id": str(uuid.uuid4()),
            "machine_id": random.choice(MACHINES),
            "shift": random.choice(SHIFTS),
            "date": fake.date_between(start_date="-7d", end_date="today").isoformat(),
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "oee": round(availability * performance * quality, 4),
            "timestamp": datetime.utcnow().isoformat(),
            "_source": "MES",
            "_event_type": "oee_metric"
        }


# ── IQMS - Quality Management System ──────────────────────────────────────────
class IQMSGenerator:
    TOPIC = "iqms.quality_tests"
    DEVIATION_TOPIC = "iqms.deviations"

    TESTS = ["HARDNESS", "DISSOLUTION", "ASSAY", "MOISTURE", "PARTICLE_SIZE", "MICROBIAL"]

    def quality_test(self) -> Dict[str, Any]:
        test = random.choice(self.TESTS)
        passed = random.random() > 0.05
        return {
            "test_id": f"QT-{uuid.uuid4().hex[:8].upper()}",
            "batch_number": f"BATCH-{fake.numerify('######')}",
            "product_code": random.choice(PRODUCTS),
            "machine_id": random.choice(MACHINES),
            "test_type": test,
            "result_value": round(random.uniform(95, 105), 3),
            "usl": 105.0,
            "lsl": 95.0,
            "result": "PASS" if passed else "FAIL",
            "analyst_id": random.choice(OPERATORS),
            "equipment_id": f"EQP-{random.randint(100, 200)}",
            "tested_at": fake.date_time_between(start_date="-30d", end_date="now").isoformat(),
            "approved_by": random.choice(OPERATORS) if passed else None,
            "_source": "IQMS",
            "_ingested_at": datetime.utcnow().isoformat(),
            "_event_type": "quality_test"
        }

    def deviation(self) -> Dict[str, Any]:
        return {
            "deviation_id": f"DEV-{uuid.uuid4().hex[:6].upper()}",
            "batch_number": f"BATCH-{fake.numerify('######')}",
            "product_code": random.choice(PRODUCTS),
            "defect_code": random.choice(DEFECT_CODES),
            "severity": random.choice(["CRITICAL", "MAJOR", "MINOR"]),
            "description": fake.sentence(nb_words=12),
            "detected_by": random.choice(OPERATORS),
            "detected_at": fake.date_time_between(start_date="-30d", end_date="now").isoformat(),
            "status": random.choice(["OPEN", "UNDER_INVESTIGATION", "CLOSED", "CLOSED"]),
            "root_cause": fake.sentence(nb_words=10) if random.random() > 0.4 else None,
            "_source": "IQMS",
            "_event_type": "deviation"
        }


# ── Historian / L2 - Time-Series Operational Data ─────────────────────────────
class HistorianGenerator:
    TOPIC = "historian.process_parameters"

    TAGS = {
        "temperature": (60, 90, "°C"),
        "pressure": (1.0, 4.0, "bar"),
        "humidity": (40, 65, "%RH"),
        "flow_rate": (5, 50, "L/min"),
        "pH": (6.5, 7.5, "pH"),
        "dissolved_oxygen": (85, 99, "%"),
        "agitator_speed": (50, 300, "RPM"),
        "jacket_temp": (15, 25, "°C"),
    }

    def process_parameter(self) -> Dict[str, Any]:
        tag = random.choice(list(self.TAGS.keys()))
        lo, hi, unit = self.TAGS[tag]
        return {
            "tag_id": f"{random.choice(MACHINES)}.{tag}",
            "tag_name": tag,
            "value": round(random.uniform(lo, hi), 4),
            "unit": unit,
            "quality": random.choices(["GOOD", "BAD", "UNCERTAIN"], weights=[95, 2, 3])[0],
            "timestamp": datetime.utcnow().isoformat(),
            "machine_id": random.choice(MACHINES),
            "_source": "Historian",
            "_event_type": "process_parameter"
        }


# ── Trackwise - CAPA / QMS ────────────────────────────────────────────────────
class TrackwiseGenerator:
    TOPIC = "trackwise.capas"
    COMPLAINT_TOPIC = "trackwise.complaints"

    def capa(self) -> Dict[str, Any]:
        opened = fake.date_time_between(start_date="-180d", end_date="-30d")
        return {
            "capa_id": f"CAPA-{uuid.uuid4().hex[:8].upper()}",
            "capa_type": random.choice(CAPA_CODES),
            "title": fake.sentence(nb_words=8),
            "description": fake.paragraph(nb_sentences=3),
            "source": random.choice(["Deviation", "Audit", "Customer Complaint", "OOS"]),
            "product_code": random.choice(PRODUCTS),
            "opened_date": opened.isoformat(),
            "target_close_date": (opened + timedelta(days=random.randint(30, 90))).isoformat(),
            "actual_close_date": (opened + timedelta(days=random.randint(30, 90))).isoformat() if random.random() > 0.3 else None,
            "owner": random.choice(OPERATORS),
            "department": random.choice(DEPARTMENTS),
            "status": random.choice(["OPEN", "IN_PROGRESS", "COMPLETED", "VERIFIED", "CLOSED"]),
            "effectiveness_verified": random.random() > 0.6,
            "_source": "Trackwise",
            "_event_type": "capa"
        }

    def complaint(self) -> Dict[str, Any]:
        return {
            "complaint_id": f"CMP-{uuid.uuid4().hex[:8].upper()}",
            "customer_id": random.choice(SAP_CUSTOMERS),
            "product_code": random.choice(PRODUCTS),
            "batch_number": f"BATCH-{fake.numerify('######')}",
            "complaint_date": fake.date_between(start_date="-90d", end_date="today").isoformat(),
            "complaint_type": random.choice(["Packaging", "Efficacy", "Labeling", "Foreign Particle", "Stability"]),
            "severity": random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
            "description": fake.paragraph(nb_sentences=2),
            "status": random.choice(["RECEIVED", "UNDER_INVESTIGATION", "RESOLVED", "CLOSED"]),
            "_source": "Trackwise",
            "_event_type": "complaint"
        }


# ── SAP ECC ───────────────────────────────────────────────────────────────────
class SAPGenerator:
    TOPIC = "sap.inventory_movements"
    PO_TOPIC = "sap.purchase_orders"
    PROD_ORDER_TOPIC = "sap.production_orders"

    MOVEMENT_TYPES = {
        "101": "GR for PO",
        "261": "GI for Production",
        "311": "Transfer Posting",
        "501": "Receipt w/o PO",
        "551": "Scrapping",
    }

    def inventory_movement(self) -> Dict[str, Any]:
        mv_type = random.choice(list(self.MOVEMENT_TYPES.keys()))
        return {
            "document_number": fake.numerify("49########"),
            "movement_type": mv_type,
            "movement_description": self.MOVEMENT_TYPES[mv_type],
            "material_code": random.choice(MATERIALS),
            "plant": random.choice(PLANTS),
            "storage_location": f"SL{random.randint(10, 50):02d}",
            "quantity": round(random.uniform(10, 5000), 3),
            "uom": random.choice(["KG", "L", "EA", "BOX"]),
            "posting_date": fake.date_between(start_date="-60d", end_date="today").isoformat(),
            "vendor_code": random.choice(SAP_VENDORS) if mv_type in ["101", "501"] else None,
            "batch_number": f"BATCH-{fake.numerify('######')}",
            "valuation_amount_inr": round(random.uniform(1000, 500000), 2),
            "_source": "SAP_ECC",
            "_event_type": "inventory_movement"
        }

    def purchase_order(self) -> Dict[str, Any]:
        po_date = fake.date_between(start_date="-90d", end_date="today")
        return {
            "po_number": fake.numerify("45########"),
            "vendor_code": random.choice(SAP_VENDORS),
            "material_code": random.choice(MATERIALS),
            "plant": random.choice(PLANTS),
            "po_date": po_date.isoformat(),
            "delivery_date": (po_date + timedelta(days=random.randint(7, 45))).isoformat(),
            "quantity": round(random.uniform(100, 10000), 2),
            "uom": random.choice(["KG", "L", "EA"]),
            "unit_price_inr": round(random.uniform(10, 10000), 2),
            "total_value_inr": round(random.uniform(10000, 5000000), 2),
            "status": random.choice(["OPEN", "PARTIAL_DELIVERY", "COMPLETE", "CLOSED"]),
            "gr_complete": random.random() > 0.4,
            "_source": "SAP_ECC",
            "_event_type": "purchase_order"
        }


# ── TMS - Training Management System ──────────────────────────────────────────
class TMSGenerator:
    TOPIC = "tms.training_completions"

    TRAININGS = [
        "GMP Basics", "21 CFR Part 11", "ALCOA+ Principles",
        "Data Integrity", "Cleanroom Gowning", "Equipment Qualification",
        "SOPs & Change Control", "HPLC Operation", "Batch Record Review",
    ]

    def training_completion(self) -> Dict[str, Any]:
        completed = random.random() > 0.1
        scheduled = fake.date_between(start_date="-90d", end_date="today")
        return {
            "record_id": str(uuid.uuid4()),
            "employee_id": random.choice(OPERATORS),
            "employee_name": fake.name(),
            "department": random.choice(DEPARTMENTS),
            "training_name": random.choice(self.TRAININGS),
            "training_category": random.choice(["GMP", "Safety", "Technical", "Regulatory"]),
            "scheduled_date": scheduled.isoformat(),
            "completion_date": (scheduled + timedelta(days=random.randint(0, 5))).isoformat() if completed else None,
            "score": random.randint(70, 100) if completed else None,
            "status": "COMPLETED" if completed else random.choice(["SCHEDULED", "IN_PROGRESS", "OVERDUE"]),
            "trainer_id": random.choice(OPERATORS),
            "training_mode": random.choice(["Classroom", "e-Learning", "OJT"]),
            "validity_months": random.choice([6, 12, 24]),
            "_source": "TMS",
            "_event_type": "training_completion"
        }


# ── Main Streaming Loop ────────────────────────────────────────────────────────
def stream_to_kafka(producer: KafkaProducer):
    """Continuously generate and publish events to Kafka topics."""
    generators = {
        "mes": MESGenerator(),
        "iqms": IQMSGenerator(),
        "historian": HistorianGenerator(),
        "trackwise": TrackwiseGenerator(),
        "sap": SAPGenerator(),
        "tms": TMSGenerator(),
    }

    event_count = 0
    while True:
        try:
            # MES Events
            mes = generators["mes"]
            producer.send(MESGenerator.TOPIC, key=str(uuid.uuid4()), value=mes.production_order())
            producer.send(MESGenerator.MACHINE_STATUS_TOPIC, value=mes.machine_status())
            producer.send(MESGenerator.OEE_TOPIC, value=mes.oee_metric())

            # IQMS Events
            iqms = generators["iqms"]
            producer.send(IQMSGenerator.TOPIC, value=iqms.quality_test())
            if random.random() < 0.1:  # 10% deviation rate
                producer.send(IQMSGenerator.DEVIATION_TOPIC, value=iqms.deviation())

            # Historian (high-frequency)
            for _ in range(5):
                producer.send(HistorianGenerator.TOPIC, value=generators["historian"].process_parameter())

            # Trackwise
            if random.random() < 0.05:
                tw = generators["trackwise"]
                producer.send(TrackwiseGenerator.TOPIC, value=tw.capa())
                producer.send(TrackwiseGenerator.COMPLAINT_TOPIC, value=tw.complaint())

            # SAP
            sap = generators["sap"]
            producer.send(SAPGenerator.TOPIC, value=sap.inventory_movement())
            if random.random() < 0.2:
                producer.send(SAPGenerator.PO_TOPIC, value=sap.purchase_order())

            # TMS
            if random.random() < 0.15:
                producer.send(TMSGenerator.TOPIC, value=generators["tms"].training_completion())

            producer.flush()
            event_count += 1

            if event_count % 100 == 0:
                logger.info(f"Published {event_count * 10}+ events across all topics")

            time.sleep(STREAM_INTERVAL_MS / 1000)

        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
            time.sleep(2)


# ── Batch Seeder (PostgreSQL) ──────────────────────────────────────────────────
def seed_postgres():
    """Seed PostgreSQL with initial batch data for all source systems."""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mes_production_orders (
            order_id VARCHAR PRIMARY KEY,
            product_code VARCHAR,
            batch_number VARCHAR,
            machine_id VARCHAR,
            operator_id VARCHAR,
            shift CHAR(1),
            plant VARCHAR,
            planned_qty INT,
            actual_qty INT,
            rejected_qty INT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status VARCHAR,
            scrap_percentage FLOAT,
            ingested_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS iqms_quality_tests (
            test_id VARCHAR PRIMARY KEY,
            batch_number VARCHAR,
            product_code VARCHAR,
            test_type VARCHAR,
            result_value FLOAT,
            usl FLOAT,
            lsl FLOAT,
            result VARCHAR,
            analyst_id VARCHAR,
            tested_at TIMESTAMP,
            ingested_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS sap_inventory (
            document_number VARCHAR PRIMARY KEY,
            movement_type VARCHAR,
            material_code VARCHAR,
            plant VARCHAR,
            quantity FLOAT,
            uom VARCHAR,
            posting_date DATE,
            valuation_amount_inr FLOAT,
            ingested_at TIMESTAMP DEFAULT NOW()
        );
    """)

    mes = MESGenerator()
    iqms = IQMSGenerator()
    sap = SAPGenerator()

    # Seed MES
    mes_rows = [mes.production_order() for _ in range(BATCH_SIZE)]
    execute_batch(cur, """
        INSERT INTO mes_production_orders VALUES
        (%(order_id)s, %(product_code)s, %(batch_number)s, %(machine_id)s,
         %(operator_id)s, %(shift)s, %(plant)s, %(planned_qty)s, %(actual_qty)s,
         %(rejected_qty)s, %(start_time)s, %(end_time)s, %(status)s, %(scrap_percentage)s)
        ON CONFLICT DO NOTHING
    """, mes_rows)

    conn.commit()
    logger.info(f"Seeded {BATCH_SIZE} rows into PostgreSQL source tables.")
    cur.close()
    conn.close()


# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    logger.info("Starting TPL Synthetic Data Generator...")

    # Seed batch data
    try:
        seed_postgres()
    except Exception as e:
        logger.warning(f"Postgres seeding skipped: {e}")

    # Start streaming to Kafka
    producer = get_kafka_producer()
    stream_to_kafka(producer)
