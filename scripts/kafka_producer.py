from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path

from confluent_kafka import Producer


ROOT_DIR = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT_DIR / "data" / "source"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
MESSAGE_DELAY_SECONDS = 0.05

TOPIC_FILES = {
    "raw.mes.events": "mes_events.csv",
    "raw.iqms.orders": "iqms_orders.csv",
    "raw.trackwise.deviations": "trackwise_deviations.csv",
    "raw.sap.orders": "sap_ecc_orders.csv",
    "raw.sop.documents": "sop_documents.csv",
}


def delivery_report(err, msg) -> None:
    if err is not None:
        raise RuntimeError(f"Delivery failed for {msg.topic()} [{msg.partition()}]: {err}")


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    published_totals: dict[str, int] = {}

    for topic, filename in TOPIC_FILES.items():
        csv_path = SOURCE_DIR / filename
        rows = load_rows(csv_path)
        total = len(rows)

        for index, row in enumerate(rows, start=1):
            producer.produce(
                topic=topic,
                key=next(iter(row.values())),
                value=json.dumps(row).encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            print(f"Published {index}/{total} messages to topic {topic}")
            time.sleep(MESSAGE_DELAY_SECONDS)

        producer.flush()
        published_totals[topic] = total

    print("\nTotal messages published per topic")
    for topic, total in published_totals.items():
        print(f"{topic}: {total}")


if __name__ == "__main__":
    main()
