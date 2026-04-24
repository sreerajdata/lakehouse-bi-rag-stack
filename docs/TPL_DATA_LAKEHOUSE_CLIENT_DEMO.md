# TPL DATA LAKEHOUSE — CLIENT DEMO SCRIPT & TECHNICAL WALKTHROUGH

This document was built from the actual repository files in `C:\Users\sharidass\Downloads\lakehouse-base-build`, including `docker-compose.yml`, the Python sources, Airflow DAGs, dbt project files, Spark jobs, Superset setup scripts, and monitoring configuration. Where the live demo script and the code differ in naming, this document follows the code as it exists now so the walkthrough stays dependable during a client call.

═══════════════════════════════════════════════
LAYER 1 — SYNTHETIC DATA GENERATOR
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the client demo problem of showing realistic pharmaceutical plant activity before live integrations to MES, IQMS, Historian/L2, Trackwise, SAP ECC, and TMS are connected. For a manufacturer, that means planners, quality teams, and plant leadership can see production orders, batch activity, deviations, inventory movements, and training records flowing through the platform without waiting on upstream system access. It is especially useful in validation workshops because it produces attributable source tags and contemporaneous ingestion timestamps that support ALCOA+ style data integrity discussions. It does not itself implement 21 CFR Part 11 controls, but it creates the mock electronic records that the downstream controls, lineage, and audit layers operate on.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `synthetic_data/generator.py`  
PURPOSE: Generates streaming manufacturing, quality, historian, CAPA, SAP, and training events and publishes them to Kafka while optionally seeding PostgreSQL.

KEY SECTION:
```python
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
```
LINE BY LINE:
- `TOPIC`, `MACHINE_STATUS_TOPIC`, and `OEE_TOPIC` define the Kafka destinations for order, machine, and OEE traffic.
- `start = fake.date_time_between(...)` creates a realistic recent production start timestamp.
- `order_id` is the synthetic production order identifier shown to the client as the manufacturing work order.
- `product_code` maps the order to one of the mock pharma SKUs.
- `batch_number` gives the lot or batch identifier that downstream quality and release steps join on.
- `machine_id` tells the client which asset or line executed the work.
- `operator_id` simulates the operator attribution needed for ALCOA+ “Attributable” discussions.
- `shift` supports shift-based demo slices in Spark, dbt, and dashboards.
- `plant` shows the site or production block context, important for multi-plant discussions.
- `planned_qty` is the target output for the order.
- `actual_qty` is the realized output and drives yield and efficiency calculations downstream.
- `rejected_qty` is the rejected material count and feeds scrap and quality KPIs.
- `start_time` and `end_time` establish order duration for throughput and OEE calculations.
- `status` models whether the order is complete, still running, or on hold.
- `scrap_percentage` gives a direct waste metric that later becomes a manufacturing KPI.
- `_source` tags the event as MES.
- `_ingested_at` stamps when the generator created the electronic record.
- `_event_type` differentiates production orders from other MES events.
WHY IT EXISTS: The method deliberately produces one record shape that is rich enough to drive bronze ingestion, silver cleansing, gold KPIs, and client-facing dashboards from a single synthetic source.

KEY SECTION:
```python
def stream_to_kafka(producer: KafkaProducer):
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
            mes = generators["mes"]
            producer.send(MESGenerator.TOPIC, key=str(uuid.uuid4()), value=mes.production_order())
            producer.send(MESGenerator.MACHINE_STATUS_TOPIC, value=mes.machine_status())
            producer.send(MESGenerator.OEE_TOPIC, value=mes.oee_metric())
            iqms = generators["iqms"]
            producer.send(IQMSGenerator.TOPIC, value=iqms.quality_test())
            if random.random() < 0.1:
                producer.send(IQMSGenerator.DEVIATION_TOPIC, value=iqms.deviation())
            for _ in range(5):
                producer.send(HistorianGenerator.TOPIC, value=generators["historian"].process_parameter())
            if random.random() < 0.05:
                tw = generators["trackwise"]
                producer.send(TrackwiseGenerator.TOPIC, value=tw.capa())
                producer.send(TrackwiseGenerator.COMPLAINT_TOPIC, value=tw.complaint())
            sap = generators["sap"]
            producer.send(SAPGenerator.TOPIC, value=sap.inventory_movement())
            if random.random() < 0.2:
                producer.send(SAPGenerator.PO_TOPIC, value=sap.purchase_order())
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
```
LINE BY LINE:
- `generators` instantiates one synthetic source-system generator per business domain.
- Each `producer.send(...)` call emits a domain event into the source-system Kafka topic.
- The conditional blocks control deviation, CAPA, complaint, SAP purchase order, and TMS event frequency so the stream looks realistic instead of uniform.
- The Historian loop emits five process parameter events per pass so machine telemetry looks higher-frequency than order events.
- `producer.flush()` forces the batch onto Kafka so the client can see activity quickly in the UI.
- `event_count` and the 100-event log line give a simple progress signal during demos.
- `time.sleep(...)` sets the stream pacing from `.env`.
WHY IT EXISTS: This loop gives the demo a believable multi-system data exhaust, with MES and Historian visibly noisier than Trackwise or TMS, which matches manufacturing reality.

KEY SECTION:
```python
def seed_postgres():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
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
    """)
    mes = MESGenerator()
    mes_rows = [mes.production_order() for _ in range(BATCH_SIZE)]
    execute_batch(cur, """
        INSERT INTO mes_production_orders VALUES
        (%(order_id)s, %(product_code)s, %(batch_number)s, %(machine_id)s,
         %(operator_id)s, %(shift)s, %(plant)s, %(planned_qty)s, %(actual_qty)s,
         %(rejected_qty)s, %(start_time)s, %(end_time)s, %(status)s, %(scrap_percentage)s)
        ON CONFLICT DO NOTHING
    """, mes_rows)
```
LINE BY LINE:
- `psycopg2.connect(**PG_CONFIG)` opens the source-style relational seed store.
- The `CREATE TABLE IF NOT EXISTS` block creates mock upstream source tables.
- `mes_rows = [mes.production_order() for _ in range(BATCH_SIZE)]` reuses the same generation logic for batch seeding.
- `execute_batch(...)` loads rows efficiently instead of one insert at a time.
- `ON CONFLICT DO NOTHING` avoids duplicate-key failures between reruns.
WHY IT EXISTS: The project supports both event streaming and source-table style seeding so Kafka demos and connector demos can coexist.

FILE: `synthetic_data/Dockerfile`  
PURPOSE: Builds the isolated Python runtime for the generator container.

KEY SECTION:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    faker==22.0.0 \
    pandas==2.1.4
COPY generator.py .
CMD ["python", "-u", "generator.py"]
```
LINE BY LINE:
- `FROM python:3.11-slim` keeps the image light.
- `WORKDIR /app` standardizes the runtime location.
- `pip install` brings in Kafka, PostgreSQL, Faker, and data utilities.
- `COPY generator.py .` adds only the generator source.
- `CMD [...]` runs unbuffered so logs show up live in Docker.
WHY IT EXISTS: The container keeps the demo reproducible without requiring local Python setup on the demo laptop.

FILE: `.env`  
PURPOSE: Sets runtime pacing for the generator.

KEY SECTION:
```env
SYNTHETIC_BATCH_SIZE=1000
SYNTHETIC_STREAM_INTERVAL_MS=500
```
LINE BY LINE:
- `SYNTHETIC_BATCH_SIZE` controls how many relational seed rows are created.
- `SYNTHETIC_STREAM_INTERVAL_MS` controls event cadence.
WHY IT EXISTS: Demo operators can slow the stream down or speed it up without editing Python.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` block:
```yaml
  synthetic-data-gen:
    build:
      context: ./synthetic_data
      dockerfile: Dockerfile
    container_name: lakehouse_synthetic_datagen
    profiles: ["synthetic", "all"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      POSTGRES_HOST: postgres
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin123
    volumes:
      - ./synthetic_data:/app
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - lakehouse_net
```

Start command:
```bash
make up-synthetic
```

Direct compose alternative:
```bash
docker compose --profile synthetic up -d
```

Verify command:
```bash
docker compose ps | grep synthetic-data-gen
docker logs lakehouse_synthetic_datagen --tail 20
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `synthetic_data/generator.py`  
HIGHLIGHT LINES: `class MESGenerator` and the full `production_order()` method  
SAY TO CLIENT: "This is where we simulate the manufacturing execution record. Every field here becomes something we can trace later, from the production order and batch number to the operator, machine, shift, yield, and scrap signal."

OPEN FILE: `synthetic_data/generator.py`  
HIGHLIGHT LINES: `stream_to_kafka()`  
SAY TO CLIENT: "This loop is broadcasting events from all six source-system domains into Kafka in near real time, so the rest of the stack sees a live plant instead of a static file import."

OPEN FILE: `synthetic_data/Dockerfile`  
HIGHLIGHT LINES: the `pip install` block and `CMD ["python", "-u", "generator.py"]`  
SAY TO CLIENT: "We package the generator as a container so the demo runs the same way on every machine and the logs stream immediately into Docker."

OPEN FILE: `.env`  
HIGHLIGHT LINES: `SYNTHETIC_BATCH_SIZE` and `SYNTHETIC_STREAM_INTERVAL_MS`  
SAY TO CLIENT: "These two settings control how much seed data we create and how fast the stream flows during the demo."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:9000`  
NAVIGATE: Kafka UI → `Topics` → `mes.production_orders` → `Messages`  
POINT TO: the newest JSON records and the topic message count increasing  
SAY TO CLIENT: "This is the synthetic MES order stream landing live. You can see the batch number, machine, operator, and timestamps changing in real time, which gives us a realistic plant signal without needing live MES connectivity."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
docker logs lakehouse_synthetic_datagen --tail 20
```
EXPECTED OUTPUT:
```text
... Kafka producer connected.
... Seeded 1000 rows into PostgreSQL source tables.
... Published 1000+ events across all topics
```
EXPLAIN: "What you are seeing is the generator first seeding mock source tables and then continuously publishing regulated manufacturing events into Kafka."

RUN:
```bash
docker exec lakehouse_kafka kafka-topics --bootstrap-server localhost:9092 --list
```
EXPECTED OUTPUT:
```text
mes.production_orders
mes.machine_status
mes.oee_metrics
iqms.quality_tests
iqms.deviations
historian.process_parameters
trackwise.capas
sap.inventory_movements
tms.training_completions
```
EXPLAIN: "These topic names mirror the source systems we care about in pharma manufacturing, so every downstream layer receives business-labeled traffic instead of generic demo messages."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
docker logs lakehouse_synthetic_datagen --tail 20
```
Expected response:
```text
Kafka producer connected.
Published ...
```

Check 2:
```bash
docker exec lakehouse_kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups
```
Expected response: topic offsets increase over time for the synthetic topics.

If the generator log does not show Kafka connectivity or topic offsets stay flat, the demo should stop and Kafka health should be checked before continuing.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data is in JSON event form on Kafka topics and in seeded source-style PostgreSQL tables. Apache Kafka picks up the streaming side immediately because the generator calls `producer.send(...)` for each domain topic and flushes the batch on every loop. Before the handoff, the record is a Python dictionary inside `generator.py`; after the handoff, it becomes a serialized UTF-8 JSON message with Kafka topic, partition, offset, and timestamp metadata attached by the broker. The handoff is triggered simply by starting the synthetic container and allowing the main loop to run.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“Now that we have realistic plant data being created continuously, let’s move one step downstream and show the message backbone that carries that data across the platform.”

═══════════════════════════════════════════════
LAYER 2 — APACHE KAFKA (Real-Time Message Bus)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the manufacturing problem of moving plant events, quality events, SAP movements, and training activity without waiting for overnight batch transfers. For a pharmaceutical manufacturer, Kafka acts as the operational event backbone between MES, IQMS, Historian/L2, Trackwise, SAP ECC, and TMS so different teams can consume the same batch signal in parallel. That matters in regulated operations because quality review, production analytics, and audit visibility all need the same original event stream rather than multiple uncontrolled copies. Kafka itself is not a 21 CFR Part 11 system of record, but it preserves the original event ordering and supports the enduring event trail later materialized into bronze.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Defines the Kafka stack services and the browser UI used in the demo.

KEY SECTION:
```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    profiles: ["ingestion", "all"]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    profiles: ["ingestion", "all"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092

  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.5.0
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_REST_PORT: 8083
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    environment:
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://kafka-connect:8083
```
LINE BY LINE:
- `zookeeper` provides broker coordination for this Compose-based Kafka deployment.
- `KAFKA_ADVERTISED_LISTENERS` exposes one address inside Docker and one for the demo laptop.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` lets the synthetic generator create topics simply by publishing.
- `schema-registry` is wired to the broker even though the current demo payloads are plain JSON.
- `kafka-connect` is prepared for Debezium and S3 sink use cases.
- `kafka-ui` ties broker, schema registry, and connect into one browser experience.
WHY IT EXISTS: This creates one demo-visible event backbone that supports live topic browsing, connector registration, and downstream consumers without extra infrastructure.

FILE: `scripts/kafka_producer.py`  
PURPOSE: Replays CSV seed files into raw Kafka topics used by the NiFi/S3 path.

KEY SECTION:
```python
TOPIC_FILES = {
    "raw.mes.events": "mes_events.csv",
    "raw.iqms.orders": "iqms_orders.csv",
    "raw.trackwise.deviations": "trackwise_deviations.csv",
    "raw.sap.orders": "sap_ecc_orders.csv",
    "raw.sop.documents": "sop_documents.csv",
}

for topic, filename in TOPIC_FILES.items():
    csv_path = SOURCE_DIR / filename
    rows = load_rows(csv_path)
    for index, row in enumerate(rows, start=1):
        producer.produce(
            topic=topic,
            key=next(iter(row.values())),
            value=json.dumps(row).encode("utf-8"),
            on_delivery=delivery_report,
        )
```
LINE BY LINE:
- `TOPIC_FILES` maps each raw topic to the CSV source file.
- `load_rows(csv_path)` reads the CSV into dictionaries.
- `producer.produce(...)` serializes each row as JSON and sends it to Kafka.
- `key=next(iter(row.values()))` uses the first column as a stable message key.
WHY IT EXISTS: The project supports both continuous synthetic streaming and deterministic CSV replay for demos where the client wants to see a finite, auditable set of source rows.

FILE: `scripts/register_connectors.sh`  
PURPOSE: Registers or updates Kafka Connect connectors from JSON files.

KEY SECTION:
```bash
for CONNECTOR_FILE in "${CONNECTOR_DIR}"/*.json; do
    CONNECTOR_NAME=$(python3 -c "import json; print(json.load(open('${CONNECTOR_FILE}'))['name'])")
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")
    if [ "$HTTP_CODE" = "200" ]; then
        curl -s -X PUT \
            -H "Content-Type: application/json" \
            -d @<(python3 -c "import json; data=json.load(open('${CONNECTOR_FILE}')); print(json.dumps(data.get('config', data)))") \
            "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"
    else
        curl -s -X POST \
            -H "Content-Type: application/json" \
            -d @"${CONNECTOR_FILE}" \
            "${CONNECT_URL}/connectors"
    fi
done
```
LINE BY LINE:
- The loop walks every connector JSON file in the connector directory.
- `CONNECTOR_NAME` pulls the connector name directly from the JSON payload.
- The `HTTP_CODE` check determines whether to create or update.
- `PUT` updates an existing connector config.
- `POST` creates a new connector.
WHY IT EXISTS: It lets the demo operator re-register connectors idempotently without clicking through the Kafka Connect API manually.

FILE: `scripts/create_kafka_connectors.sh`  
PURPOSE: Creates S3 sink connectors that land raw Kafka topics into SeaweedFS bronze storage.

KEY SECTION:
```bash
"connector.class": "io.confluent.connect.s3.S3SinkConnector",
"topics": "${topic}",
"s3.bucket.name": "bronze",
"store.url": "http://seaweedfs-s3:8333",
"format.class": "io.confluent.connect.s3.format.json.JsonFormat",
"topics.dir": "${topics_dir}",
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false"
```
LINE BY LINE:
- `S3SinkConnector` writes Kafka topics straight to object storage.
- `topics` selects the Kafka topic to sink.
- `s3.bucket.name` points at the bronze bucket.
- `store.url` makes SeaweedFS behave like the object store target.
- `JsonFormat` preserves raw JSON.
- `topics.dir` separates output by business domain.
WHY IT EXISTS: It provides a no-code landing pattern from event bus to raw storage for clients who want immediate object-store persistence.

FILE: `configs/kafka-connect/connectors/mes-postgres-connector.json`  
PURPOSE: Debezium connector for seeded MES-style PostgreSQL data.

KEY SECTION:
```json
{
  "name": "mes-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.dbname": "lakehouse_meta",
    "topic.prefix": "cdc.mes",
    "table.include.list": "public.mes_production_orders",
    "plugin.name": "pgoutput",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
  }
}
```
LINE BY LINE:
- `PostgresConnector` tells Kafka Connect to watch PostgreSQL change data.
- `topic.prefix` names the emitted CDC topic family.
- `table.include.list` narrows capture to MES production orders.
- `plugin.name` uses PostgreSQL logical decoding.
- The `unwrap` transform strips Debezium envelope fields and leaves the row payload.
WHY IT EXISTS: It demonstrates how a relational source system could join the streaming backbone without custom application code.

FILE: `configs/kafka-connect/connectors/iqms-postgres-connector.json`  
PURPOSE: Debezium connector for IQMS quality tables.  
KEY SECTION:
```json
"name": "iqms-postgres-connector",
"table.include.list": "public.iqms_quality_tests",
"topic.prefix": "cdc.iqms"
```
LINE BY LINE:
- The connector is narrowed to IQMS test rows.
- The topic prefix keeps IQMS CDC traffic separate from MES.
WHY IT EXISTS: It mirrors the regulated quality path separately from manufacturing orders.

FILE: `configs/kafka-connect/connectors/tms-postgres-connector.json`  
PURPOSE: Debezium connector for TMS training completions.  
KEY SECTION:
```json
"name": "tms-postgres-connector",
"table.include.list": "public.tms_training_completions",
"topic.prefix": "cdc.tms"
```
LINE BY LINE:
- The connector captures training records.
- The topic prefix isolates training compliance events.
WHY IT EXISTS: It lets the platform show workforce compliance signals moving on the same event bus as manufacturing signals.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` blocks:
```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: lakehouse_zookeeper
    profiles: ["ingestion", "all"]

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: lakehouse_kafka
    profiles: ["ingestion", "all"]

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    container_name: lakehouse_schema_registry
    profiles: ["ingestion", "all"]

  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.5.0
    container_name: lakehouse_kafka_connect
    profiles: ["ingestion", "all"]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: lakehouse_kafka_ui
    profiles: ["ingestion", "all"]
```

Start command:
```bash
make up-ingestion
```

Direct compose alternative:
```bash
docker compose --profile ingestion up -d
```

Verify commands:
```bash
docker compose ps | grep kafka
curl -s http://localhost:8083/connectors | python3 -m json.tool
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `docker-compose.yml`  
HIGHLIGHT LINES: the `zookeeper`, `kafka`, `schema-registry`, `kafka-connect`, and `kafka-ui` services  
SAY TO CLIENT: "This is the real-time backbone of the platform. Every source-system event lands here first so multiple downstream processes can consume the same original manufacturing signal."

OPEN FILE: `scripts/kafka_producer.py`  
HIGHLIGHT LINES: `TOPIC_FILES` and the `producer.produce(...)` loop  
SAY TO CLIENT: "This is our replay path for deterministic source files. It lets us drive the same event bus from CSV when a client wants a fixed demo dataset instead of an infinite stream."

OPEN FILE: `scripts/register_connectors.sh`  
HIGHLIGHT LINES: the connector create-or-update loop  
SAY TO CLIENT: "This script turns connector deployment into a repeatable step, so CDC and S3 landing can be enabled without hand-configuring each connector through the API."

OPEN FILE: `configs/kafka-connect/connectors/mes-postgres-connector.json`  
HIGHLIGHT LINES: `connector.class`, `table.include.list`, `topic.prefix`, and `transforms.unwrap.*`  
SAY TO CLIENT: "This is how a seeded upstream source table would join the event backbone using Debezium, with the raw change envelope flattened into business-ready messages."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:9000`  
NAVIGATE: Kafka UI → `Clusters` → `local` → `Topics`  
POINT TO: the list of topics such as `mes.production_orders`, `iqms.quality_tests`, `historian.process_parameters`, and `sap.inventory_movements`  
SAY TO CLIENT: "Each topic is a business event stream from a source system. This means manufacturing, quality, SAP, and training events can all be consumed independently without duplicating integrations."

OPEN URL: `http://localhost:9000`  
NAVIGATE: Kafka UI → `Connect` → choose the configured connector if registered  
POINT TO: connector status and tasks  
SAY TO CLIENT: "This is where we verify the source-to-bus and bus-to-storage integration pieces are healthy and moving data."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
docker exec lakehouse_kafka kafka-topics --bootstrap-server localhost:9092 --list
```
EXPECTED OUTPUT:
```text
mes.production_orders
mes.machine_status
mes.oee_metrics
iqms.quality_tests
iqms.deviations
historian.process_parameters
trackwise.capas
sap.inventory_movements
tms.training_completions
```
EXPLAIN: "This confirms the manufacturing, quality, historian, CAPA, SAP, and training event channels all exist and are ready for downstream consumers."

RUN:
```bash
curl -s http://localhost:8083/connectors
```
EXPECTED OUTPUT:
```json
["mes-postgres-connector","iqms-postgres-connector","tms-postgres-connector"]
```
EXPLAIN: "These connectors are how relational source changes can also be brought onto the same event backbone."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
docker exec lakehouse_kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```
Expected response: broker version metadata prints without error.

Check 2:
```bash
curl -s http://localhost:8081/subjects
```
Expected response:
```json
[]
```
or a JSON array of registered subjects, but never a connection error.

Check 3:
```bash
curl -s http://localhost:8083/connectors | python3 -m json.tool
```
Expected response: valid JSON array or object from Kafka Connect.

If broker metadata fails, Kafka is not healthy. If Kafka Connect does not answer HTTP, CDC and sink demos should not continue.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data is a Kafka message stream containing raw JSON payloads plus broker metadata such as topic, partition, and offset. Apache NiFi is one of the next tools that can pick it up, either by consuming topics such as `raw.mes.events` or by publishing file-derived records into topics like `raw.nifi.ingest`. The handoff happens technically through the Kafka broker API, either from the generator’s `producer.send(...)` calls or from NiFi processors like `PublishKafka` and `ConsumeKafka`. Before the handoff, the record is an in-memory Python dict or CSV row; after the handoff, it is a durable event on a named topic that any subscribed consumer can replay.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“We’ve now shown the event backbone, so let’s look at the second ingestion path: the file and document flow that operations teams often use alongside real-time messaging.”

═══════════════════════════════════════════════
LAYER 3 — APACHE NIFI (CSV File and Document Ingestion)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the common pharma problem where not every source system emits real-time events; some systems still deliver CSV extracts, SOP indexes, and plant documents. For a manufacturer, NiFi is the operational intake desk for IQMS exports, document indices, and file drops from MES-adjacent or quality processes, and it can also route those files into the same downstream bronze landing zone. It is relevant to MES, IQMS, Trackwise, SAP ECC, and document-controlled quality content because many regulated processes still arrive as files even when core systems are digital. ALCOA+ matters here because NiFi helps preserve source, timing, and routing context on landed records.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Runs the NiFi service for file ingestion and Kafka bridging.

KEY SECTION:
```yaml
  nifi:
    image: apache/nifi:1.23.2
    container_name: lakehouse_nifi
    profiles: ["ingestion", "all"]
    environment:
      NIFI_WEB_HTTP_PORT: 8090
      NIFI_CLUSTER_IS_NODE: "false"
      SINGLE_USER_CREDENTIALS_USERNAME: ${NIFI_USER:-admin}
      SINGLE_USER_CREDENTIALS_PASSWORD: ${NIFI_PASS:-adminadminadmin}
    volumes:
      - ./data:/opt/nifi/data
      - nifi_conf:/opt/nifi/nifi-current/conf
```
LINE BY LINE:
- `apache/nifi:1.23.2` defines the NiFi runtime.
- `NIFI_WEB_HTTP_PORT` publishes the browser UI.
- The single-user credentials enable a quick demo login.
- `./data:/opt/nifi/data` makes the source CSV files visible to NiFi processors.
WHY IT EXISTS: It turns the repository’s `data/source` folder into a live intake directory the demo can show in the browser.

FILE: `scripts/create_nifi_flow.py`  
PURPOSE: Creates two NiFi process groups programmatically: one for CSV file ingestion and one for Kafka-to-S3 routing.

KEY SECTION:
```python
flow1 = create_process_group(root_pg_id, f"FLOW 1 - CSV File Ingestion {flow_suffix}", 40.0, 80.0)
csv_reader = create_controller_service(flow1_id, "CSV Reader", "org.apache.nifi.csv.CSVReader")
get_file = create_processor(flow1_id, "GetFile", "org.apache.nifi.processors.standard.GetFile", "nifi-standard-nar", 0.0, 0.0)
split_record = create_processor(flow1_id, "SplitRecord", "org.apache.nifi.processors.standard.SplitRecord", "nifi-standard-nar", 260.0, 0.0)
convert_record = create_processor(flow1_id, "ConvertRecord", "org.apache.nifi.processors.standard.ConvertRecord", "nifi-standard-nar", 520.0, 0.0)
publish_kafka = create_processor(flow1_id, "PublishKafka", "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6", "nifi-kafka-2-6-nar", 780.0, 0.0)
put_s3_ingest = create_processor(flow1_id, "PutS3Object", "org.apache.nifi.processors.aws.s3.PutS3Object", "nifi-aws-nar", 1040.0, 0.0)
```
LINE BY LINE:
- `create_process_group(...)` creates a visible flow canvas group in NiFi.
- `CSV Reader` gives NiFi schema-aware CSV parsing.
- `GetFile` watches the data directory.
- `SplitRecord` breaks bulk CSV files into one-record units.
- `ConvertRecord` converts CSV records into JSON.
- `PublishKafka` publishes file records into Kafka.
- `PutS3Object` lands those JSON records in SeaweedFS.
WHY IT EXISTS: This is the exact no-code-style flow the client expects to see for legacy file onboarding.

KEY SECTION:
```python
update_processor(
    get_file["id"],
    {
        "Input Directory": "/opt/nifi/data/source",
        "File Filter": ".*\\.csv",
        "Keep Source File": "true",
        "Recurse Subdirectories": "false",
    },
    [],
)
update_processor(
    publish_kafka["id"],
    {"bootstrap.servers": "kafka:9092", "topic": "raw.nifi.ingest", "use-transactions": "false"},
    ["failure"],
)
update_processor(
    put_s3_ingest["id"],
    {
        "Bucket": "bronze",
        "Object Key": "nifi-ingest/${now():format(\"yyyyMMddHHmmssSSS\")}-${filename}-${fragment.index}.json",
        "Endpoint Override URL": "http://seaweedfs-s3:8333",
    },
    ["failure"],
)
```
LINE BY LINE:
- `Input Directory` points NiFi to the repo’s source folder.
- `File Filter` narrows processing to CSVs.
- `Keep Source File` allows safe re-runs during demos.
- The `PublishKafka` topic is `raw.nifi.ingest`.
- The `PutS3Object` key pattern stamps time, source file, and split index into the object name.
WHY IT EXISTS: The flow preserves traceability from original file to event message to bronze object.

FILE: `data/source/mes_events.csv`  
PURPOSE: Example CSV NiFi can ingest.

KEY SECTION:
```csv
event_id,machine_id,batch_id,product_code,parameter_name,parameter_value,unit,operator_id,shift,event_ts,status
EVT-000001,MCH-003,BATCH-01053,API-100,temperature,22.04,C,OP022,C,2026-04-17 11:26:04,PASS
```
LINE BY LINE:
- The header defines a structured plant-event schema.
- The row represents a single machine-level manufacturing parameter event.
WHY IT EXISTS: It gives the file-ingestion demo a deterministic source record.

FILE: `data/source/iqms_orders.csv`  
PURPOSE: Example order file for file-based quality/manufacturing intake.  
KEY SECTION:
```csv
order_id,product_code,batch_id,quantity,uom,planned_start,actual_start,actual_end,status,line_id
ORD-00001,API-500,BATCH-01259,3005,L,2026-04-17 12:17:26,2026-04-17 14:12:26,2026-04-18 06:12:26,COMPLETE,LINE-1
```
WHY IT EXISTS: It lets the client see how a structured business extract can enter the same platform as streaming events.

FILE: `data/source/trackwise_deviations.csv`  
PURPOSE: Example quality deviation file.  
KEY SECTION:
```csv
deviation_id,batch_id,product_code,deviation_type,severity,description,reported_by,reported_ts,status,resolution_ts
DEV-00001,BATCH-01168,API-400,CONTAMINATION,MINOR,...
```
WHY IT EXISTS: It supports a regulated quality-use-case ingestion path without live Trackwise access.

FILE: `data/source/sap_ecc_orders.csv`  
PURPOSE: Example SAP inventory/order extract.  
KEY SECTION:
```csv
po_number,material_code,plant,storage_location,planned_qty,actual_qty,uom,posting_date,cost_center,total_cost,currency
PO-500000,MAT-200,PLANT03,QC02,2416,2759,KG,2026-03-31,CC-584,54626.41,USD
```
WHY IT EXISTS: It lets the demo show supply-chain and inventory data arriving through the same file-intake path.

FILE: `data/source/sop_documents.csv`  
PURPOSE: Example controlled-document index file.  
KEY SECTION:
```csv
doc_id,doc_type,title,version,effective_date,author,department,file_path,page_count
DOC-0001,CAPA,Quality Capa 1,2.1,2024-08-08,N. Rao,Quality,/docs/quality/capa/0001.pdf,115
```
WHY IT EXISTS: It makes document governance visible as a first-class ingestion domain.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` block:
```yaml
  nifi:
    image: apache/nifi:1.23.2
    container_name: lakehouse_nifi
    profiles: ["ingestion", "all"]
    environment:
      NIFI_WEB_HTTP_PORT: 8090
      NIFI_CLUSTER_IS_NODE: "false"
      SINGLE_USER_CREDENTIALS_USERNAME: ${NIFI_USER:-admin}
      SINGLE_USER_CREDENTIALS_PASSWORD: ${NIFI_PASS:-adminadminadmin}
    volumes:
      - ./data:/opt/nifi/data
      - nifi_conf:/opt/nifi/nifi-current/conf
      - nifi_content:/opt/nifi/nifi-current/content_repository
      - nifi_database:/opt/nifi/nifi-current/database_repository
      - nifi_flowfile:/opt/nifi/nifi-current/flowfile_repository
      - nifi_provenance:/opt/nifi/nifi-current/provenance_repository
    ports:
      - "8090:8090"
```

Start command:
```bash
make up-ingestion
```

Direct compose alternative:
```bash
docker compose --profile ingestion up -d
```

Verify commands:
```bash
docker compose ps | grep nifi
curl -s http://localhost:8090/nifi-api/system-diagnostics | python3 -m json.tool
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `scripts/create_nifi_flow.py`  
HIGHLIGHT LINES: the flow creation block for `GetFile → SplitRecord → ConvertRecord → PublishKafka → PutS3Object`  
SAY TO CLIENT: "This is the file-ingestion flow. We watch a source directory, split each CSV row into an individual business record, convert it to JSON, publish it to Kafka, and land it in the bronze bucket."

OPEN FILE: `scripts/create_nifi_flow.py`  
HIGHLIGHT LINES: the `ConsumeKafka`, `EvaluateJSON`, `RouteOnAttribute`, and `PutS3Object PASS/FAIL` processors  
SAY TO CLIENT: "The second flow shows NiFi acting as a streaming router as well. Here it consumes MES events from Kafka, classifies them by status, and writes passing versus failing events to different bronze prefixes."

OPEN FILE: `data/source/sop_documents.csv`  
HIGHLIGHT LINES: the header and first one or two rows  
SAY TO CLIENT: "This is a good example of the kind of controlled-document index or operational export that still arrives as a file in many regulated environments."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:8090`  
NAVIGATE: log in with `admin / adminadminadmin` → NiFi canvas → open the latest `FLOW 1 - CSV File Ingestion ...` group  
POINT TO: the processors `GetFile`, `SplitRecord`, `ConvertRecord`, `PublishKafka`, and `PutS3Object` with green running icons  
SAY TO CLIENT: "This is the visual file-ingestion path. A dropped CSV is split into atomic records, converted to JSON, and then forwarded both to Kafka and to our bronze landing zone."

OPEN URL: `http://localhost:8090`  
NAVIGATE: back to root canvas → open the latest `FLOW 2 - Kafka Consumer Flow ...` group  
POINT TO: `RouteOnAttribute` and the two S3 sinks for pass and fail routing  
SAY TO CLIENT: "This second flow shows NiFi doing operational triage, separating passing process events from exception events before they land."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
python scripts/create_nifi_flow.py
```
EXPECTED OUTPUT:
```text
Using NiFi at http://localhost:8090/nifi-api, root PG = ...
NiFi flow created successfully.
FLOW 1 processors are RUNNING.
FLOW 2 processors are RUNNING.
Objects found under s3://bronze/nifi-ingest/:
```
EXPLAIN: "This script builds the NiFi demo flow programmatically so we know the exact processors, routes, and destinations are in place every time."

RUN:
```bash
curl -s http://localhost:8090/nifi-api/system-diagnostics
```
EXPECTED OUTPUT: JSON containing `systemDiagnostics` and component health data.  
EXPLAIN: "This tells us the NiFi runtime is alive and able to process file and Kafka traffic."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
curl -s http://localhost:8090/nifi-api/flow/process-groups/root | python3 -m json.tool
```
Expected response: JSON containing process group flow metadata.

Check 2:
```bash
docker run --rm --network lakehouse-base-build_lakehouse_net -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=admin123 amazon/aws-cli:2.15.0 --endpoint-url http://seaweedfs-s3:8333 s3 ls s3://bronze/nifi-ingest/ --recursive
```
Expected response: one or more JSON objects listed under the `nifi-ingest/` prefix.

If the processors are not running or no files appear under `nifi-ingest/`, the file-ingestion demo path is broken.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data is either line-level JSON published into Kafka or JSON objects stored in the SeaweedFS bronze bucket. SeaweedFS bronze storage is the next layer that picks it up for raw landing, while Spark later reads those landed files or the staged CSV sources to create Iceberg tables. Technically, the handoff happens through NiFi `PutS3Object` writes to the `bronze` bucket and through `PublishKafka` into `raw.nifi.ingest` or `ConsumeKafka` from `raw.mes.events`. Before the handoff, the data is a CSV row or raw topic message; after it, the data is a JSON object with source file lineage and a timestamped object key in object storage.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“With the file intake flow in place, the next thing to show is where all raw data lands before we start transforming it.”

═══════════════════════════════════════════════
LAYER 4 — SEAWEEDFS BRONZE STORAGE (Raw Data Landing Zone)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the pharma data-landing problem by giving the platform one raw storage zone for MES, IQMS, Historian/L2, Trackwise, SAP ECC, TMS, and document data. For a manufacturer, that means original events and files can be retained before cleansing, which is essential when quality, compliance, and supply-chain teams need to trace back to the original record. This is where ALCOA+ becomes tangible because the platform can preserve original payloads, timestamps, and source tags in a durable object store. While SeaweedFS itself is storage infrastructure rather than a Part 11 application, it underpins the enduring and available characteristics expected in regulated data flows.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Defines the SeaweedFS master, volume, filer, S3 gateway, and bucket initializer.

KEY SECTION:
```yaml
  seaweedfs-master:
    image: chrislusf/seaweedfs:3.63
    command: "master -ip=seaweedfs-master -ip.bind=0.0.0.0 -port=9333 -mdir=/data"

  seaweedfs-volume:
    image: chrislusf/seaweedfs:3.63
    command: "volume -ip.bind=0.0.0.0 -ip=seaweedfs-volume -mserver=seaweedfs-master:9333 -port=8082 -dir=/data -max=100"

  seaweedfs-filer:
    image: chrislusf/seaweedfs:3.63
    command: "filer -ip.bind=0.0.0.0 -ip=seaweedfs-filer -master=seaweedfs-master:9333 -port=8888"

  seaweedfs-s3:
    image: chrislusf/seaweedfs:3.63
    command: "s3 -ip.bind=0.0.0.0 -filer=seaweedfs-filer:8888 -port=8333 -config=/etc/seaweedfs/s3.json"

  seaweedfs-init:
    image: amazon/aws-cli:2.15.0
    command: /scripts/init-seaweedfs.sh
```
LINE BY LINE:
- `master` manages SeaweedFS cluster metadata.
- `volume` stores actual object chunks.
- `filer` gives a filesystem-style namespace and browser-friendly view.
- `s3` exposes the same storage through an S3-compatible API.
- `seaweedfs-init` runs the bucket bootstrapping script after the S3 gateway is healthy.
WHY IT EXISTS: The platform needs one object-store backend that can serve NiFi, Spark, Iceberg, Tika, and document indexing without a proprietary appliance.

FILE: `configs/seaweedfs/s3.json`  
PURPOSE: Defines SeaweedFS S3 identities and permissions.

KEY SECTION:
```json
{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        {
          "accessKey": "admin",
          "secretKey": "admin123"
        }
      ],
      "actions": ["Admin", "Read", "ReadAcp", "Write", "WriteAcp"]
    }
  ]
}
```
LINE BY LINE:
- `identities` defines S3 users.
- `admin` is the primary credential used by Spark, NiFi, and setup scripts.
- `actions` grants read and write control.
WHY IT EXISTS: It centralizes the access keys the rest of the stack reuses.

FILE: `scripts/init-seaweedfs.sh`  
PURPOSE: Waits for S3 availability, creates buckets, and adds bronze folder prefixes.

KEY SECTION:
```sh
BUCKETS="bronze silver gold lakehouse-bronze lakehouse-silver lakehouse-gold lakehouse-models lakehouse-docs milvus-bucket"
for BUCKET in $BUCKETS; do
  if aws --endpoint-url=$ENDPOINT s3 ls "s3://$BUCKET" > /dev/null 2>&1; then
    echo "Bucket $BUCKET already exists."
  else
    aws --endpoint-url=$ENDPOINT s3 mb "s3://$BUCKET"
    echo "Created bucket: $BUCKET"
  fi
done
for SYSTEM in mes iqms historian trackwise sap tms nifi_flows docs; do
  aws --endpoint-url=$ENDPOINT s3api put-object --bucket bronze --key "${SYSTEM}/" > /dev/null 2>&1 || true
done
```
LINE BY LINE:
- `BUCKETS=...` lists all object-store buckets used by the platform.
- The first loop creates each bucket idempotently.
- The second loop creates domain prefixes like `mes/`, `iqms/`, and `sap/`.
WHY IT EXISTS: It guarantees the storage layout exists before ingestion starts.

FILE: `storage-demo.html`  
PURPOSE: Gives the presenter a static storage map for explaining where bronze, silver, and gold tables live.

KEY SECTION:
```html
<a href="http://localhost:8888/buckets/" target="_blank">Buckets</a>
<a href="http://localhost:8888/buckets/lakehouse-gold/warehouse/" target="_blank">Warehouse Root</a>
<details>
  <summary>mes_events</summary>
  <ul><li><code>data/</code> and <code>metadata/</code></li></ul>
</details>
```
LINE BY LINE:
- The browser links point straight at the filer UI.
- The `details` blocks map logical tables to physical Iceberg folders.
WHY IT EXISTS: It helps a client understand object layout without making them navigate raw filer paths during the demo.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` blocks:
```yaml
  seaweedfs-master: { ... }
  seaweedfs-volume: { ... }
  seaweedfs-filer: { ... }
  seaweedfs-s3: { ... }
  seaweedfs-init: { ... }
```

Start command:
```bash
make up-core
make init-buckets
```

Direct compose alternative:
```bash
docker compose --profile core up -d
docker compose --profile core run --rm seaweedfs-init
```

Verify commands:
```bash
docker compose ps | grep seaweedfs
curl -s http://localhost:9333/cluster/status | python3 -m json.tool
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `docker-compose.yml`  
HIGHLIGHT LINES: the `seaweedfs-master`, `seaweedfs-volume`, `seaweedfs-filer`, `seaweedfs-s3`, and `seaweedfs-init` services  
SAY TO CLIENT: "This is the raw landing layer for the lakehouse. We expose the same storage as a cluster, a filer, and an S3-compatible endpoint so every tool in the stack can use it."

OPEN FILE: `scripts/init-seaweedfs.sh`  
HIGHLIGHT LINES: the bucket creation loop and the system prefix loop  
SAY TO CLIENT: "We bootstrap the storage namespace before data arrives, so bronze, silver, gold, documents, and model artifacts all have dedicated locations from the first run."

OPEN FILE: `configs/seaweedfs/s3.json`  
HIGHLIGHT LINES: the `admin` identity  
SAY TO CLIENT: "This is the shared S3 identity the ingestion and processing tools use to write into the landing zone."

OPEN FILE: `storage-demo.html`  
HIGHLIGHT LINES: the bronze, silver, and gold root links  
SAY TO CLIENT: "This gives us a simple visual map from the business layers to the actual object-store folders underneath."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:9333/cluster/status`  
NAVIGATE: open the URL directly  
POINT TO: cluster health JSON and node information  
SAY TO CLIENT: "This tells us the object-store control plane is up and ready to accept landings."

OPEN URL: `http://localhost:8888/buckets/bronze/`  
NAVIGATE: filer root → `buckets` → `bronze`  
POINT TO: prefixes such as `mes/`, `iqms/`, `sap/`, and `nifi-ingest/`  
SAY TO CLIENT: "This is the raw landing zone. Each source-system family gets its own prefix so we can retain original records before transformation."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
curl -s http://localhost:9333/cluster/status
```
EXPECTED OUTPUT:
```json
{"IsLeader":true,...}
```
EXPLAIN: "This confirms the SeaweedFS control plane is healthy."

RUN:
```bash
docker run --rm --network lakehouse-base-build_lakehouse_net -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=admin123 amazon/aws-cli:2.15.0 --endpoint-url http://seaweedfs-s3:8333 s3 ls
```
EXPECTED OUTPUT:
```text
2026-.. bronze
2026-.. silver
2026-.. gold
2026-.. lakehouse-bronze
2026-.. lakehouse-silver
2026-.. lakehouse-gold
```
EXPLAIN: "This shows the object store is not just reachable, it already has the managed buckets the lakehouse expects."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
curl -s http://localhost:9333/cluster/status | python3 -m json.tool
```
Expected response: valid JSON with `IsLeader`.

Check 2:
```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8333/
```
Expected response:
```text
HTTP 200
```
or `HTTP 403`, which still proves the S3 endpoint is alive.

Check 3:
```bash
docker run --rm --network lakehouse-base-build_lakehouse_net -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=admin123 amazon/aws-cli:2.15.0 --endpoint-url http://seaweedfs-s3:8333 s3 ls s3://bronze
```
Expected response: one or more prefixes such as `mes/`, `iqms/`, or `nifi-ingest/`.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data exists as raw objects in S3-compatible SeaweedFS and, for Iceberg-backed bronze tables, as object-store folders containing `data/` and `metadata/` files. Apache Spark is the next tool that picks the data up, either by reading staged CSVs from `s3a://bronze/source` or by writing streaming topic payloads into Iceberg table locations backed by SeaweedFS. The technical handoff is triggered by Spark jobs such as `create_bronze_tables.py` and `bronze_kafka_to_iceberg.py`, which use the SeaweedFS S3A endpoint and Iceberg warehouse paths. Before the handoff, the data is raw JSON or CSV-derived objects; after the handoff, it becomes typed Iceberg bronze tables with schema and partition metadata.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“Now that the raw landing zone is in place, let’s show the engine that turns those raw landings into structured lakehouse tables.”

═══════════════════════════════════════════════
LAYER 5 — APACHE SPARK (Bronze Transformation Engine)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the pharmaceutical need to standardize raw manufacturing and quality data before it can be trusted for analysis. For MES, IQMS, Trackwise, SAP ECC, TMS, and document index feeds, Spark is where raw fields are typed, metadata columns are added, and the bronze tables are created in a consistent format. In business terms, this is the step that turns scattered raw records into a governed landing foundation that downstream quality, productivity, and inventory reporting can rely on. It is also where ALCOA+ reinforcement becomes visible because source tags, ingestion times, hashes, and partitions are attached to the raw payloads before wider consumption.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Starts the Spark master and workers.

KEY SECTION:
```yaml
  spark-master:
    image: bitnami/spark:3.4
    container_name: lakehouse_spark_master
    profiles: ["processing", "all"]
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark-master
      - SPARK_MASTER_PORT=7077
      - SPARK_MASTER_WEBUI_PORT=8181

  spark-worker-1:
    <<: *spark-common

  spark-worker-2:
    <<: *spark-common
```
LINE BY LINE:
- `spark-master` defines the coordinator.
- The master web UI is exposed at `8181`.
- The workers inherit common Spark settings and attach to the master URL.
WHY IT EXISTS: It creates a small distributed compute cluster the client can see live in the browser.

FILE: `configs/spark/spark-defaults.conf`  
PURPOSE: Configures Spark for Iceberg and SeaweedFS S3A.

KEY SECTION:
```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type=hive
spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083
spark.hadoop.fs.s3a.endpoint=http://seaweedfs-s3:8333
spark.hadoop.fs.s3a.path.style.access=true
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4
```
LINE BY LINE:
- The first four lines enable Iceberg SQL behavior and connect Spark to the Hive metastore.
- The S3A lines tell Spark to treat SeaweedFS as its object-store backend.
- The `spark.jars.packages` line ensures Iceberg and S3A libraries are available.
WHY IT EXISTS: Without these settings, Spark would not be able to create or query Iceberg tables on SeaweedFS.

FILE: `scripts/create_bronze_tables.py`  
PURPOSE: Reads staged source files and creates Iceberg bronze tables.

KEY SECTION:
```python
mes_df = (
    spark.read.option("header", True).csv(f"{BRONZE_BASE}/mes_events.csv")
    .select(
        "event_id",
        "machine_id",
        "batch_id",
        "product_code",
        "parameter_name",
        F.col("parameter_value").cast("double").alias("parameter_value"),
        "unit",
        "operator_id",
        "shift",
        F.to_timestamp("event_ts").alias("event_ts"),
        "status",
    )
    .transform(lambda df: add_metadata(df, "MES", "csv_seed"))
    .transform(lambda df: cluster_for_partition(df, "event_ts"))
)
write_table(spark, mes_df, "lakehouse.bronze.mes_events", f"{ICEBERG_BASE}/mes_events", "days(event_ts)")
```
LINE BY LINE:
- `spark.read...csv(...)` reads the raw CSV.
- `.select(...)` narrows and types the business columns.
- `add_metadata(...)` adds `_source`, `_ingested_at`, and `_nifi_flow`.
- `cluster_for_partition(...)` improves organization within partitions.
- `write_table(...)` materializes the Iceberg bronze table.
WHY IT EXISTS: This is the cleanest initial bronze build path for the seeded CSV sources used elsewhere in the project.

FILE: `scripts/kafka_to_bronze.py`  
PURPOSE: Reads Kafka topics in batch mode and appends ALCOA+-tagged raw payloads into bronze Iceberg tables.

KEY SECTION:
```python
processed_df = (
    df.select(
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("value").cast("string").alias("_raw_payload"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp")
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_system", F.lit(topic.split('.')[0].upper()))
    .withColumn("_row_hash", F.sha2(F.col("_raw_payload"), 256))
    .withColumn("_ingest_year", F.year(F.col("_ingested_at")))
)
processed_df.writeTo(table).append()
```
LINE BY LINE:
- The `select(...)` block preserves Kafka metadata.
- `_ingested_at` timestamps the bronze record creation.
- `_source_system` derives the source family from the topic name.
- `_row_hash` adds a stable integrity hash.
- The partition columns make the table operationally manageable.
- `.append()` lands the micro-batch into Iceberg.
WHY IT EXISTS: This is the raw-payload preservation path that best supports lineage and replay.

FILE: `configs/airflow/dags/spark_jobs/bronze_kafka_to_iceberg.py`  
PURPOSE: Defines the streaming-style Spark bronze ingestion pattern used from Airflow.

KEY SECTION:
```python
query = (
    enriched_df.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", f"{output_path}/_checkpoint")
    .trigger(processingTime="60 seconds")
    .start()
)
```
LINE BY LINE:
- `writeStream` turns the Spark job into a streaming consumer.
- `foreachBatch(write_batch)` gives precise Iceberg write control.
- `checkpointLocation` makes the stream restartable.
- `trigger(processingTime="60 seconds")` processes on a one-minute cadence.
WHY IT EXISTS: It shows how the bronze engine can run as a structured stream instead of only as a batch loader.

FILE: `scripts/verify_bronze.sql`  
PURPOSE: Checks row counts for the bronze tables after Spark loads them.

KEY SECTION:
```sql
SELECT 'mes_events' AS table_name, COUNT(*) AS row_count FROM iceberg.bronze.mes_events
UNION ALL
SELECT 'iqms_orders', COUNT(*) FROM iceberg.bronze.iqms_orders
UNION ALL
SELECT 'trackwise_deviations', COUNT(*) FROM iceberg.bronze.trackwise_deviations
```
LINE BY LINE:
- Each `SELECT` returns a bronze table name and row count.
- `UNION ALL` composes one validation result set.
WHY IT EXISTS: It gives the demo operator one fast post-load quality gate.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` blocks:
```yaml
  spark-master: { ... }
  spark-worker-1: { ... }
  spark-worker-2: { ... }
```

Start command:
```bash
make up-processing
```

Direct compose alternative:
```bash
docker compose --profile processing up -d
```

Verify commands:
```bash
docker compose ps | grep spark
curl -s http://localhost:8181 | head
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `configs/spark/spark-defaults.conf`  
HIGHLIGHT LINES: the Iceberg catalog block and SeaweedFS S3A block  
SAY TO CLIENT: "These settings are what let Spark treat object storage as a governed table warehouse instead of just a file dump."

OPEN FILE: `scripts/create_bronze_tables.py`  
HIGHLIGHT LINES: the `mes_df` build and `write_table(...)` call  
SAY TO CLIENT: "This is the bronze creation step. We cast the raw fields, add source metadata, and write them as Iceberg tables that downstream tools can query consistently."

OPEN FILE: `scripts/kafka_to_bronze.py`  
HIGHLIGHT LINES: the `_row_hash`, `_source_system`, and partition columns  
SAY TO CLIENT: "For streaming topics we preserve the raw payload and attach integrity and partition metadata, which is exactly what quality and audit teams want in the raw landing layer."

OPEN FILE: `scripts/verify_bronze.sql`  
HIGHLIGHT LINES: the row-count union query  
SAY TO CLIENT: "This is our quick proof that bronze has data before we hand anything to dbt."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:8181`  
NAVIGATE: Spark master home page  
POINT TO: the master status and worker count  
SAY TO CLIENT: "This is the distributed compute layer turning raw landed records into table-backed lakehouse assets."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
docker exec lakehouse_spark_master spark-submit /opt/lakehouse/scripts/create_bronze_tables.py
```
EXPECTED OUTPUT:
```text
+---------+-------------------+-------------------+
|row_count |min_event_ts       |max_event_ts       |
+---------+-------------------+-------------------+
|...      |...                |...                |
```
EXPLAIN: "Spark has read the raw source data, typed it, and written it into bronze Iceberg tables, and this output shows the row and timestamp range for one of those tables."

RUN:
```bash
docker exec lakehouse_trino trino --server http://localhost:8080 --catalog iceberg --execute "SELECT * FROM bronze.mes_events LIMIT 5"
```
EXPECTED OUTPUT: five typed bronze rows.  
EXPLAIN: "This confirms the Bronze Spark load produced a queryable table, not just raw files."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
curl -s http://localhost:8181
```
Expected response: Spark master UI HTML.

Check 2:
```bash
docker exec lakehouse_trino trino --server http://localhost:8080 --catalog iceberg --execute "SELECT COUNT(*) FROM bronze.mes_events"
```
Expected response: a non-zero count.

Check 3:
```bash
docker exec lakehouse_trino trino --server http://localhost:8080 --catalog iceberg --file /opt/airflow/scripts/verify_bronze.sql
```
Expected response: counts for all listed bronze tables.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data is in typed Apache Iceberg bronze tables backed by SeaweedFS object storage. Apache Airflow is the next tool that picks the process up by orchestrating when the Spark jobs run and when dbt is allowed to transform bronze into silver and gold. The technical handoff happens through the Airflow DAG’s `SparkSubmitOperator`, followed by Trino validation and dbt tasks. Before the handoff, the data is raw objects or Kafka payloads; after the handoff, it is bronze tables with stable schemas, partitions, and metadata ready for orchestrated transformation.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“We now have raw tables in the lakehouse, so the next step is to show the control layer that sequences the entire pipeline.”

═══════════════════════════════════════════════
LAYER 6 — APACHE AIRFLOW (Pipeline Orchestration)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the operational problem of running the manufacturing pipeline in a repeatable, inspectable order. For a pharmaceutical manufacturer, Airflow is where plant, quality, and supply-chain flows are scheduled, monitored, retried, and evidenced across MES, IQMS, Trackwise, SAP ECC, TMS, and document processing. That matters in regulated operations because teams need a clear record of when data moved, which step passed, and which step failed before a dashboard or report is trusted. Airflow is not the electronic-signature system, but it gives the execution audit trail around the pipeline that quality and IT both care about.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Runs Redis and the Airflow webserver, scheduler, worker, and init job.

KEY SECTION:
```yaml
  airflow-init:
    <<: *airflow-common
    command:
      - -c
      - |
        airflow db init
        airflow users create --username admin --password admin \
          --firstname Admin --lastname User --role Admin --email admin@lakehouse.local
        airflow connections add spark_lakehouse --conn-type spark --conn-host 'spark://spark-master' --conn-port 7077
        airflow connections add trino_lakehouse --conn-uri 'trino://admin@trino:8080/iceberg'

  airflow-webserver:
    <<: *airflow-common
    command: webserver

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  airflow-worker:
    <<: *airflow-common
    command:
      - bash
      - -c
      - |
        rm -f /opt/airflow/airflow-worker.pid
        exec airflow celery worker
```
LINE BY LINE:
- `airflow-init` initializes the metadata DB, creates the admin user, and registers Spark and Trino connections.
- `airflow-webserver` hosts the browser UI.
- `airflow-scheduler` evaluates schedules and launches tasks.
- `airflow-worker` executes Celery tasks.
WHY IT EXISTS: This gives the demo a production-style orchestrator instead of a single local cron job.

FILE: `dockerfiles/airflow/Dockerfile`  
PURPOSE: Builds Airflow with the providers and tools needed by this stack.

KEY SECTION:
```dockerfile
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-trino \
    great-expectations \
    confluent-kafka \
    pandas \
    faker \
    pyspark==3.4.3 \
    dbt-trino==1.7.1
```
LINE BY LINE:
- The provider packages let Airflow call Spark, Kafka-related code, and Trino.
- `great-expectations` supports downstream data quality checks.
- `pyspark` and `dbt-trino` let Airflow run jobs inside the same image.
WHY IT EXISTS: It turns Airflow into the orchestrator for the full medallion flow rather than just a scheduler shell.

FILE: `configs/airflow/dags/medallion_pipeline.py`  
PURPOSE: Defines the end-to-end medallion demo pipeline.

KEY SECTION:
```python
with DAG(
    dag_id="medallion_full_pipeline",
    description="End-to-end lakehouse demo pipeline",
    schedule="*/15 * * * *",
    catchup=False,
) as dag:
    generate_source_data = PythonOperator(...)
    publish_to_kafka = PythonOperator(...)
    check_kafka_topics = PythonOperator(...)
    spark_bronze_ingest = SparkSubmitOperator(...)
    verify_bronze_counts = TrinoOperator(...)
    dbt_run_silver = BashOperator(...)
    dbt_test_silver = BashOperator(...)
    run_gx_validation = PythonOperator(...)
    dbt_run_gold = BashOperator(...)
    dbt_test_gold = BashOperator(...)
    notify_pipeline_complete = PythonOperator(...)
```
LINE BY LINE:
- `dag_id="medallion_full_pipeline"` is the actual DAG name to use in the demo.
- The schedule runs every 15 minutes.
- The task list sequences the pipeline from ingest checks through Spark, dbt silver, GX, dbt gold, and notification.
WHY IT EXISTS: It expresses the demo in the same order the business expects to hear it.

KEY SECTION:
```python
(
    generate_source_data
    >> publish_to_kafka
    >> check_kafka_topics
    >> spark_bronze_ingest
    >> verify_bronze_counts
    >> dbt_run_silver
    >> dbt_test_silver
    >> run_gx_validation
    >> dbt_run_gold
    >> dbt_test_gold
    >> notify_pipeline_complete
)
```
LINE BY LINE:
- Each `>>` arrow expresses an execution dependency.
- Bronze must succeed before silver.
- Silver tests and GX must pass before gold runs.
- The final notification summarizes bronze, silver, and gold row counts.
WHY IT EXISTS: It makes the end-to-end pipeline auditable and visible in the Airflow graph.

FILE: `configs/airflow/dags/iceberg_maintenance.py`  
PURPOSE: Runs daily Iceberg housekeeping.

KEY SECTION:
```python
spark.sql("""
    CALL lakehouse.system.expire_snapshots(
        table => '{table}',
        older_than => TIMESTAMP '...',
        retain_last => 5
    )
""")
spark.sql("""CALL lakehouse.system.rewrite_data_files(table => '{table}')""")
spark.sql("""CALL lakehouse.system.rewrite_manifests(table => '{table}')""")
```
LINE BY LINE:
- `expire_snapshots` keeps metadata growth under control.
- `rewrite_data_files` compacts silver tables.
- `rewrite_manifests` improves gold read performance.
WHY IT EXISTS: It shows the client the platform also manages itself after the demo pipeline runs.

FILE: `configs/airflow/dags/pdf_processing_pipeline.py`  
PURPOSE: Orchestrates SeaweedFS → Tika → bronze → Milvus document processing.

KEY SECTION:
```python
t_list_pdfs = PythonOperator(task_id="list_new_pdfs", ...)
t_process = PythonOperator(task_id="process_pdfs", ...)
t_index = PythonOperator(task_id="trigger_milvus_indexer", ...)
t_notify = BashOperator(task_id="notify_completion", ...)
t_list_pdfs >> t_process >> t_index >> t_notify
```
LINE BY LINE:
- The DAG lists incoming PDFs.
- It extracts text and writes bronze JSON.
- It triggers indexing for retrieval workflows.
- It ends with a completion notification.
WHY IT EXISTS: It proves Airflow is orchestrating both tabular and document pipelines in the same control plane.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` blocks:
```yaml
  redis: { ... }
  airflow-init: { ... }
  airflow-webserver: { ... }
  airflow-scheduler: { ... }
  airflow-worker: { ... }
```

Start command:
```bash
make up-processing
```

Direct compose alternative:
```bash
docker compose --profile processing up -d
```

Verify commands:
```bash
docker compose ps | grep airflow
curl -s http://localhost:8280/health | python3 -m json.tool
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `configs/airflow/dags/medallion_pipeline.py`  
HIGHLIGHT LINES: the `with DAG(...)` block and the task dependency chain  
SAY TO CLIENT: "This is the orchestration spine. It makes sure we only move from bronze to silver to gold when each upstream step has completed."

OPEN FILE: `dockerfiles/airflow/Dockerfile`  
HIGHLIGHT LINES: the provider install block  
SAY TO CLIENT: "We load Spark, Trino, dbt, and Great Expectations support into the same orchestration image so one control plane can run the entire stack."

OPEN FILE: `configs/airflow/dags/iceberg_maintenance.py`  
HIGHLIGHT LINES: `expire_snapshots`, `rewrite_data_files`, and `rewrite_manifests`  
SAY TO CLIENT: "This is what keeps the lakehouse healthy after initial ingestion, especially once the volume of snapshots and manifests starts to grow."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:8280`  
NAVIGATE: log in with `admin / admin` → `DAGs` → `medallion_full_pipeline` → `Graph`  
POINT TO: the Spark, dbt, GX, and notification tasks in sequence  
SAY TO CLIENT: "This is the live pipeline control plane. Every stage from bronze ingestion to business mart publication is visible, ordered, and retryable here."

OPEN URL: `http://localhost:8280`  
NAVIGATE: `DAGs` → `iceberg_maintenance` → `Grid`  
POINT TO: the maintenance run history  
SAY TO CLIENT: "This shows the platform also takes care of routine housekeeping, not just the first-pass ingestion flow."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
curl -s http://localhost:8280/health
```
EXPECTED OUTPUT:
```json
{"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy"},...}
```
EXPLAIN: "The control plane is healthy, which means scheduling, metadata, and task dispatch are all available."

RUN:
```bash
docker exec lakehouse_airflow_web airflow dags list
```
EXPECTED OUTPUT:
```text
medallion_full_pipeline
iceberg_maintenance
pdf_processing_pipeline
```
EXPLAIN: "These are the runnable orchestration workflows we can show during the demo."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
curl -s http://localhost:8280/health | python3 -m json.tool
```
Expected response: `metadatabase` and `scheduler` statuses are `healthy`.

Check 2:
```bash
docker exec lakehouse_airflow_web airflow dags list
```
Expected response: the three DAG IDs above are listed.

Check 3:
```bash
curl -s -u admin:admin http://localhost:8280/api/v1/dags
```
Expected response: JSON with a `dags` array.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data itself has not changed format yet; what changes is that the movement from bronze to silver and gold is now under controlled orchestration. dbt is the next tool that picks the data up, and Airflow does the handoff by running `dbt run --select silver`, `dbt test --select silver`, `dbt run --select gold`, and `dbt test --select gold` as Bash tasks after the Spark and Trino validations complete. Before the handoff, the business data is sitting in bronze Iceberg tables; after the handoff, dbt begins producing conformed silver models and presentation-ready gold marts. The handoff event is the completion of `verify_bronze_counts` inside the Airflow DAG.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“With the orchestration layer in place, we can now show how the raw bronze tables are turned into clean silver models and business-ready gold marts.”

═══════════════════════════════════════════════
LAYER 7 — DBT TRANSFORMATIONS (Silver and Gold Layers)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the business problem of turning raw plant and quality records into trusted metrics a pharmaceutical manufacturer can actually run on. For MES, IQMS, Trackwise, SAP ECC, and TMS, dbt is where raw event structures become clean conformed datasets in silver and then become OEE, batch quality, CAPA, inventory, and training marts in gold. This is where manufacturing language finally becomes report-ready language: yield, pass rate, closure rate, risk status, and compliance status. ALCOA+ and 21 CFR Part 11 become genuinely relevant here because the transformation layer preserves traceability fields, applies quality tests, and prepares datasets that can stand up in regulated review.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `dockerfiles/dbt/Dockerfile`  
PURPOSE: Provides the dbt runtime container.

KEY SECTION:
```dockerfile
FROM python:3.11-slim
WORKDIR /usr/app/dbt
RUN pip install --no-cache-dir dbt-trino==1.7.1
CMD ["tail", "-f", "/dev/null"]
```
LINE BY LINE:
- The image is minimal and dedicated to dbt.
- `dbt-trino` is the adapter used to build against Trino/Iceberg.
- The container stays alive so Airflow or operators can run commands inside it.
WHY IT EXISTS: It makes dbt a stable service in the stack rather than a workstation dependency.

FILE: `dbt/dbt_project.yml`  
PURPOSE: Defines dbt project structure and default materializations.

KEY SECTION:
```yaml
models:
  enterprise_lakehouse:
    silver:
      +materialized: incremental
      +unique_key: _row_hash
      +on_schema_change: append_new_columns
      +schema: silver
      +incremental_strategy: merge
    gold:
      +materialized: table
      +schema: gold
```
LINE BY LINE:
- Silver models default to incremental merge behavior.
- `_row_hash` is the dedupe key for incremental logic.
- Schema changes append new columns instead of failing builds.
- Gold models materialize as full tables for reporting stability.
WHY IT EXISTS: It separates conformed incremental processing from presentation-ready marts.

FILE: `dbt/profiles.yml`  
PURPOSE: Connects dbt to Trino.

KEY SECTION:
```yaml
dev:
  type: trino
  host: trino
  port: 8080
  database: iceberg
  schema: silver
  user: admin
  threads: 4
```
LINE BY LINE:
- `type: trino` selects the adapter.
- `database: iceberg` points at the Iceberg catalog.
- `schema: silver` is the default working schema.
- `threads: 4` allows parallel model execution.
WHY IT EXISTS: It gives dbt direct SQL access to the lakehouse query engine.

FILE: `dbt/macros/generate_schema_name.sql`  
PURPOSE: Controls schema naming.

KEY SECTION:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```
LINE BY LINE:
- If no custom schema is set, dbt uses the target schema.
- If a custom schema is set, it uses that explicit value.
WHY IT EXISTS: It keeps bronze, silver, and gold schemas clean instead of name-prefixed.

FILE: `dbt/models/silver/sources.yml`  
PURPOSE: Declares bronze sources consumed by silver.

KEY SECTION:
```yaml
sources:
  - name: bronze
    database: iceberg
    schema: bronze
    tables:
      - name: mes_events
      - name: mes_production_orders
      - name: iqms_deviations
      - name: iqms_orders
      - name: iqms_quality_tests
```
LINE BY LINE:
- The source name is `bronze`.
- The database is the Trino Iceberg catalog.
- Each listed table is a dbt source relation available to silver models.
WHY IT EXISTS: It creates explicit lineage from raw bronze tables into silver SQL.

FILE: `dbt/models/silver/silver_mes_events.sql`  
PURPOSE: Cleans and enriches raw MES event records.

KEY SECTION:
```sql
with mes as (
    select ... upper(coalesce(cast(status as varchar), 'WARNING')) as status ...
),
iqms as (
    select ... upper(coalesce(cast(status as varchar), 'PLANNED')) as order_status ...
),
joined as (
    select mes.event_id, mes.machine_id, mes.batch_id, mes.product_code, ...
    from mes
    left join iqms
        on mes.batch_id = iqms.batch_id
       and iqms.batch_rank = 1
),
scored as (
    select *,
        case
            when parameter_name = 'temperature' and parameter_value between 15 and 35 then 'PASS'
            when parameter_name = 'pressure' and parameter_value between 0.5 and 10 then 'PASS'
            else 'FAIL'
        end as dq_status
    from joined
)
select * from scored
```
LINE BY LINE:
- `mes` standardizes MES fields.
- `iqms` brings in order context.
- `joined` links machine events to the latest batch order.
- `scored` creates a simple data-quality status by parameter range.
WHY IT EXISTS: It produces one silver table that joins process events to production context.

FILE: `dbt/models/silver/silver_mes_production_orders.sql`  
PURPOSE: Parses raw MES order payloads from bronze Kafka landing.

KEY SECTION:
```sql
try(json_extract_scalar(_raw_payload, '$.order_id')) as order_id,
try(cast(json_extract_scalar(_raw_payload, '$.planned_qty') as integer)) as planned_qty,
case
    when planned_qty > 0
        then round(cast(actual_qty as double) / planned_qty * 100, 2)
end as yield_pct,
row_number() over (partition by order_id order by _ingested_at desc) as rn
```
LINE BY LINE:
- `json_extract_scalar(...)` parses fields from the raw bronze JSON payload.
- The `yield_pct` calculation turns quantities into a manufacturing KPI.
- `row_number()` deduplicates by latest arrival.
WHY IT EXISTS: It converts immutable raw bronze payloads into a conformed order table.

FILE: `dbt/models/silver/silver_production_orders.sql`  
PURPOSE: Joins IQMS and SAP signals into one production order view.  
KEY SECTION:
```sql
left join sap
    on iqms.product_code = sap.product_code
...
round((coalesce(actual_qty, 0) / nullif(coalesce(planned_qty, 0), 0)) * 100, 2) as yield,
case
    when (...) >= 0.95 then 'HIGH'
    when (...) >= 0.85 then 'MEDIUM'
    else 'LOW'
end as production_efficiency
```
WHY IT EXISTS: It fuses manufacturing order execution with SAP cost and stock context.

FILE: `dbt/models/silver/silver_quality_events.sql`  
PURPOSE: Joins Trackwise deviations to batch event context.  
KEY SECTION:
```sql
inner join mes_batches
    on deviations.batch_id = mes_batches.batch_id
...
case deviations.severity
    when 'CRITICAL' then 3
    when 'MAJOR' then 2
    when 'MINOR' then 1
end as severity_score
```
WHY IT EXISTS: It creates a quality-event table with production context and severity scoring.

FILE: `dbt/models/silver/silver_iqms_deviations.sql`  
PURPOSE: Parses IQMS deviation payloads from raw bronze JSON.  
KEY SECTION:
```sql
case
    when upper(trim(status)) in ('OPEN', 'UNDER_INVESTIGATION') then true
    else false
end as is_open
```
WHY IT EXISTS: It gives later risk marts a consistent open-versus-closed signal.

FILE: `dbt/models/silver/silver_iqms_quality_tests.sql`  
PURPOSE: Parses IQMS quality-test payloads and derives pass/fail measures.  
KEY SECTION:
```sql
case when upper(trim(result)) = 'PASS' then true else false end as pass_fail_flag,
case
    when result_value > usl then 'ABOVE_USL'
    when result_value < lsl then 'BELOW_LSL'
    else 'WITHIN_SPEC'
end as spec_status
```
WHY IT EXISTS: It standardizes test outcomes for quality reporting.

FILE: `dbt/models/silver/silver_sap_inventory.sql`  
PURPOSE: Standardizes SAP inventory/order data.  
KEY SECTION:
```sql
actual_qty - planned_qty as variance_qty,
greatest(actual_qty, 0) as closing_stock,
round(total_cost / nullif(actual_qty, 0), 2) as unit_cost
```
WHY IT EXISTS: It converts SAP movement/order fields into reporting-ready inventory measures.

FILE: `dbt/models/silver/silver_tms_training.sql`  
PURPOSE: Standardizes TMS training records.  
KEY SECTION:
```sql
upper(cast(status as varchar)) as status,
cast(validity_months as integer) as validity_months
```
WHY IT EXISTS: It turns training records into compliance-ready rows for monthly completion reporting.

FILE: `dbt/models/silver/silver_trackwise_capas.sql`  
PURPOSE: Parses Trackwise CAPA payloads.  
KEY SECTION:
```sql
case
    when actual_close_date is null and current_date > cast(target_close_date as date)
        then true
    else false
end as is_overdue
```
WHY IT EXISTS: It makes overdue CAPA logic explicit for compliance reporting.

FILE: `dbt/models/silver/schema.yml`  
PURPOSE: Declares silver tests.

KEY SECTION:
```yaml
  - name: silver_mes_events
    columns:
      - name: event_id
        tests:
          - not_null
          - unique
```
LINE BY LINE:
- `silver_mes_events` is the model under test.
- `event_id` must be non-null and unique.
WHY IT EXISTS: It encodes the minimum trust requirements for silver.

FILE: `configs/airflow/dags/ge_checkpoints/run_checkpoint.py`  
PURPOSE: Runs SQL-based Great Expectations style checks against silver tables.

KEY SECTION:
```python
if exp_type == "expect_column_values_to_not_be_null":
    row = conn.execute(text(f"SELECT COUNT(*) AS total, COUNT({column}) AS non_null FROM {table_name}")).fetchone()
elif exp_type == "expect_column_values_to_be_between":
    ...
elif exp_type == "expect_column_values_to_be_in_set":
    ...
```
LINE BY LINE:
- The script loads the checkpoint YAML.
- It connects to Trino.
- It evaluates each expectation as SQL against the silver table.
WHY IT EXISTS: It creates an extra quality gate between silver and gold.

FILE: `configs/airflow/dags/ge_checkpoints/mes_silver_checkpoint.yml`  
PURPOSE: Defines MES silver quality expectations.  
KEY SECTION:
```yaml
- expectation_type: expect_column_values_to_be_between
  kwargs:
    column: yield_pct
    min_value: 0
    max_value: 120
```
WHY IT EXISTS: It encodes business-valid yield boundaries for production orders.

FILE: `configs/airflow/dags/ge_checkpoints/iqms_silver_checkpoint.yml`  
PURPOSE: Defines IQMS silver quality expectations.  
KEY SECTION:
```yaml
- expectation_type: expect_column_values_to_be_in_set
  kwargs:
    column: result
    value_set: ["PASS", "FAIL"]
```
WHY IT EXISTS: It prevents invalid test-result values from moving downstream.

FILE: `configs/airflow/dags/ge_checkpoints/sap_silver_checkpoint.yml`  
PURPOSE: Defines SAP silver validation rules.  
KEY SECTION:
```yaml
- expectation_type: expect_column_values_to_not_be_null
  kwargs:
    column: posting_date
```
WHY IT EXISTS: It enforces basic completeness for inventory reporting.

FILE: `dbt/models/gold/gold_batch_summary.sql`  
PURPOSE: Creates batch-level release status and production summary.  
KEY SECTION:
```sql
case
    when coalesce(quality_summary.critical_deviations, 0) > 0 or mes_batches.dq_fail_events > 0 then 'REJECTED'
    when coalesce(quality_summary.open_deviations, 0) > 0 then 'UNDER_REVIEW'
    else 'RELEASED'
end as batch_status
```
WHY IT EXISTS: It expresses release logic in batch-review language the client understands.

FILE: `dbt/models/gold/gold_manufacturing_oee_mart.sql`  
PURPOSE: Builds machine/day OEE metrics.  
KEY SECTION:
```sql
round(
    (cast(p.completed_orders as double) / nullif(p.total_orders, 0))
    * (coalesce(p.avg_yield_pct, 0) / 100)
    * (coalesce(q.pass_rate_pct, 0) / 100),
    4
) as oee_score
```
WHY IT EXISTS: It turns production and quality signals into the core manufacturing KPI.

FILE: `dbt/models/gold/gold_oee_dashboard.sql`  
PURPOSE: Creates hour-windowed trend data for the dashboard.  
KEY SECTION:
```sql
date_trunc('hour', event_ts) as hour_window,
round(sum(case when status = 'PASS' then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as quality_rate
```
WHY IT EXISTS: It gives Superset a simple time-series-friendly dataset.

FILE: `dbt/models/gold/gold_production_efficiency.sql`  
PURPOSE: Creates weekly line-level efficiency and trend.  
KEY SECTION:
```sql
lag(avg_yield_pct) over (partition by line_id order by week_start) as prior_week_yield
```
WHY IT EXISTS: It supports trend conversations with production leadership.

FILE: `dbt/models/gold/gold_quality_kpis.sql`  
PURPOSE: Creates product/day quality KPI summaries.  
KEY SECTION:
```sql
round(sum(case when batch_summary.batch_status = 'RELEASED' then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as right_first_time_pct
```
WHY IT EXISTS: It creates the right-first-time and deviation-rate metrics quality teams expect.

FILE: `dbt/models/gold/gold_quality_risk_mart.sql`  
PURPOSE: Creates product/batch quality risk scores and RAG status.  
KEY SECTION:
```sql
case
    when coalesce(d.critical_deviation_count, 0) > 0 then 'RED'
    when coalesce(d.open_deviations, 0) > 2 or coalesce(qt.pass_rate, 100) < 90 then 'AMBER'
    else 'GREEN'
end as risk_rag_status
```
WHY IT EXISTS: It converts complex quality signals into an executive risk readout.

FILE: `dbt/models/gold/gold_compliance_capa_mart.sql`  
PURPOSE: Creates monthly CAPA closure and compliance status by department.  
KEY SECTION:
```sql
case
    when cs.closure_rate_pct >= 90 then 'GREEN'
    when cs.closure_rate_pct >= 70 then 'AMBER'
    else 'RED'
end as compliance_rag_status
```
WHY IT EXISTS: It gives QA leadership a simple department-level CAPA scorecard.

FILE: `dbt/models/gold/gold_sap_inventory_mart.sql`  
PURPOSE: Aggregates inventory results by material/plant/location/month.  
KEY SECTION:
```sql
sum(variance_qty) as variance_qty,
sum(closing_stock) as closing_stock,
round(avg(unit_cost), 2) as avg_unit_cost
```
WHY IT EXISTS: It turns SAP rows into usable inventory metrics.

FILE: `dbt/models/gold/gold_training_compliance_mart.sql`  
PURPOSE: Aggregates training compliance by department and month.  
KEY SECTION:
```sql
round(cast(count_if(status = 'COMPLETED') as double) / nullif(count(*), 0) * 100, 2) as completion_rate_pct
```
WHY IT EXISTS: It gives a simple training compliance KPI for regulated operations.

FILE: `dbt/models/gold/schema.yml`  
PURPOSE: Declares gold-level test expectations.

KEY SECTION:
```yaml
  - name: gold_batch_summary
    columns:
      - name: batch_id
        tests:
          - not_null
          - unique
```
LINE BY LINE:
- `gold_batch_summary` must have a stable batch key.
- `batch_id` must exist and be unique.
WHY IT EXISTS: Gold marts must be trustworthy enough for dashboards and executive review.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` block:
```yaml
  dbt:
    build:
      context: ./dockerfiles/dbt
      dockerfile: Dockerfile
    container_name: lakehouse_dbt
    profiles: ["processing", "all"]
    entrypoint: ["tail", "-f", "/dev/null"]
    volumes:
      - ./dbt:/usr/app/dbt
```

Start command:
```bash
make up-processing
```

Direct compose alternative:
```bash
docker compose --profile processing up -d
```

Verify commands:
```bash
docker compose ps | grep dbt
docker exec lakehouse_dbt dbt debug --profiles-dir /usr/app/dbt
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `dbt/dbt_project.yml`  
HIGHLIGHT LINES: the silver incremental config and gold table config  
SAY TO CLIENT: "This is the contract for how we build the medallion layers: incremental and merge-friendly in silver, presentation-ready in gold."

OPEN FILE: `dbt/models/silver/silver_mes_production_orders.sql`  
HIGHLIGHT LINES: JSON extraction, `yield_pct`, and deduplication logic  
SAY TO CLIENT: "This model is where raw production-order payloads become a clean manufacturing order table with yield and duration already calculated."

OPEN FILE: `dbt/models/gold/gold_manufacturing_oee_mart.sql`  
HIGHLIGHT LINES: the OEE calculation block  
SAY TO CLIENT: "This is the step where production execution and quality outcomes become the OEE score the plant leadership team cares about."

OPEN FILE: `configs/airflow/dags/ge_checkpoints/mes_silver_checkpoint.yml`  
HIGHLIGHT LINES: the `yield_pct` and `status` expectations  
SAY TO CLIENT: "We don’t just transform the data; we gate it with business-valid expectations before it becomes reportable."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:8280`  
NAVIGATE: Airflow → `medallion_full_pipeline` → task instance for `dbt_run_silver` or `dbt_run_gold` → logs  
POINT TO: successful dbt execution logs  
SAY TO CLIENT: "This is the transformation layer running under orchestration. The silver models standardize the data and the gold models publish business-ready marts."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
docker exec lakehouse_dbt dbt run --select silver --profiles-dir /usr/app/dbt
```
EXPECTED OUTPUT:
```text
Completed successfully
Done. PASS=...
```
EXPLAIN: "We’ve just built the conformed silver layer from the bronze tables."

RUN:
```bash
docker exec lakehouse_dbt dbt run --select gold --profiles-dir /usr/app/dbt
```
EXPECTED OUTPUT:
```text
Completed successfully
```
EXPLAIN: "This step turns the conformed layer into business marts for OEE, quality, CAPA, inventory, and training."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
docker exec lakehouse_dbt dbt test --select silver --profiles-dir /usr/app/dbt
```
Expected response: dbt tests pass.

Check 2:
```bash
docker exec lakehouse_dbt dbt test --select gold --profiles-dir /usr/app/dbt
```
Expected response: gold tests pass.

Check 3:
```bash
python configs/airflow/dags/ge_checkpoints/run_checkpoint.py --checkpoint mes_silver_checkpoint
```
Expected response:
```text
Checkpoint: mes_silver_checkpoint — PASSED
```

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data is in conformed silver Iceberg tables and aggregated gold Iceberg marts. Apache Trino is the next tool that picks it up by exposing those tables for SQL access under the `iceberg` catalog. The technical handoff happens automatically once dbt materializes the relations; Trino does not need a copy step because it reads the same Iceberg metadata and object-store files through the Hive metastore. Before the handoff, the data is a dbt-built table relation in silver or gold; after the handoff, it is a queryable analytical dataset accessible by Trino, Superset, Grafana, and other consumers.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“The lakehouse marts are built at this point, so now we can show the SQL layer that makes those tables available to dashboards and users.”

═══════════════════════════════════════════════
LAYER 8 — APACHE TRINO (Query Engine)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the business need for one consistent SQL access point across bronze, silver, and gold without moving data into a separate warehouse. For a pharmaceutical manufacturer, that means analysts, data engineers, dashboard tools, and even AI assistants can query MES, IQMS, Trackwise, SAP ECC, and TMS-derived marts from one governed engine. It also supports audit and investigation workflows because teams can inspect bronze raw history and gold business aggregates through the same SQL interface. In regulated environments this matters because traceability improves when everyone is querying the same authoritative tables rather than downloading side copies.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Starts Hive Metastore and Trino.

KEY SECTION:
```yaml
  hive-metastore:
    build:
      context: ./dockerfiles/hive-metastore
      dockerfile: Dockerfile
    entrypoint: ["/bin/bash", "/hive-init.sh"]

  trino:
    image: trinodb/trino:435
    container_name: lakehouse_trino
    profiles: ["lakehouse", "all"]
    volumes:
      - ./configs/trino/config.properties:/etc/trino/config.properties
      - ./configs/trino/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties
```
LINE BY LINE:
- `hive-metastore` provides the table catalog backing Iceberg.
- `trino` mounts its engine and catalog config from the repo.
WHY IT EXISTS: Trino depends on the metastore to discover Iceberg tables and schemas.

FILE: `dockerfiles/hive-metastore/Dockerfile`  
PURPOSE: Extends Hive with AWS/S3A JARs so it can talk to SeaweedFS.

KEY SECTION:
```dockerfile
ADD https://repo1.maven.org/.../hadoop-aws-3.3.4.jar /opt/hive/lib/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/.../aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar
```
LINE BY LINE:
- The JARs add S3A support into Hive.
WHY IT EXISTS: Without these JARs, the metastore could not resolve SeaweedFS-backed warehouse locations.

FILE: `configs/hive/hive-site.xml`  
PURPOSE: Configures the Hive metastore and warehouse path.

KEY SECTION:
```xml
<property>
  <name>hive.metastore.uris</name>
  <value>thrift://hive-metastore:9083</value>
</property>
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>s3a://lakehouse-gold/warehouse</value>
</property>
<property>
  <name>fs.s3a.endpoint</name>
  <value>http://seaweedfs-s3:8333</value>
</property>
```
LINE BY LINE:
- `hive.metastore.uris` exposes the thrift endpoint.
- `hive.metastore.warehouse.dir` points to the Iceberg warehouse root.
- `fs.s3a.endpoint` tells Hive where object storage lives.
WHY IT EXISTS: It is the metadata bridge between Trino and SeaweedFS-backed Iceberg tables.

FILE: `scripts/hive-init.sh`  
PURPOSE: Ensures the necessary S3 JARs exist before Hive starts.

KEY SECTION:
```bash
if [ ! -f "$S3A_JAR" ]; then
  curl -L .../hadoop-aws-3.3.4.jar -o "$S3A_JAR"
fi
if [ ! -f "$SDK_JAR" ]; then
  curl -L .../aws-java-sdk-bundle-1.12.262.jar -o "$SDK_JAR"
fi
exec /entrypoint.sh
```
LINE BY LINE:
- Each `if` guards a required dependency.
- `exec /entrypoint.sh` starts Hive only after the dependencies are present.
WHY IT EXISTS: It hardens the metastore boot sequence for object-store access.

FILE: `configs/trino/config.properties`  
PURPOSE: Core Trino coordinator settings.

KEY SECTION:
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=4GB
```
LINE BY LINE:
- The node acts as the coordinator.
- It is allowed to schedule work locally as well.
- The HTTP server listens on port 8080 inside the container.
- Query memory is capped.
WHY IT EXISTS: It configures a single-node Trino coordinator suitable for this demo stack.

FILE: `configs/trino/node.properties`  
PURPOSE: Trino node identity.

KEY SECTION:
```properties
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir=/data/trino
```
WHY IT EXISTS: It gives Trino a persistent node identity and data directory.

FILE: `configs/trino/catalog/iceberg.properties`  
PURPOSE: Connects Trino’s `iceberg` catalog to Hive and SeaweedFS.

KEY SECTION:
```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://seaweedfs-s3:8333
hive.s3.aws-access-key=admin
hive.s3.path-style-access=true
```
LINE BY LINE:
- `connector.name=iceberg` enables Iceberg support.
- `iceberg.catalog.type=hive_metastore` tells Trino to resolve tables through Hive.
- The `hive.s3.*` lines give object-store connectivity.
WHY IT EXISTS: This is the exact bridge from Trino SQL to the Iceberg tables built by Spark and dbt.

FILE: `scripts/verify_bronze.sql`  
PURPOSE: Example Trino-side validation query for the demo.  
KEY SECTION:
```sql
SELECT 'mes_events' AS table_name, COUNT(*) AS row_count FROM iceberg.bronze.mes_events
```
WHY IT EXISTS: It is a clean proof that Trino can query the lakehouse.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` blocks:
```yaml
  hive-metastore: { ... }
  trino: { ... }
```

Start command:
```bash
make up-lakehouse
```

Direct compose alternative:
```bash
docker compose --profile lakehouse up -d
```

Verify commands:
```bash
docker compose ps | grep trino
curl -s http://localhost:8180/v1/info | python3 -m json.tool
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `configs/trino/catalog/iceberg.properties`  
HIGHLIGHT LINES: the metastore URI and SeaweedFS S3 settings  
SAY TO CLIENT: "This is the bridge that lets Trino see the same Iceberg tables Spark and dbt wrote, without copying data anywhere else."

OPEN FILE: `configs/hive/hive-site.xml`  
HIGHLIGHT LINES: `hive.metastore.warehouse.dir` and `fs.s3a.endpoint`  
SAY TO CLIENT: "This is where the warehouse root and object-store endpoint are anchored on the metadata side."

OPEN FILE: `configs/trino/config.properties`  
HIGHLIGHT LINES: the coordinator and memory settings  
SAY TO CLIENT: "These are the core engine settings that make Trino the shared SQL layer for the whole platform."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:8180/ui/`  
NAVIGATE: open the Trino web UI directly  
POINT TO: query history and cluster information if queries have run  
SAY TO CLIENT: "This is the federated SQL engine sitting on top of the lakehouse tables. Every dashboard and SQL consumer is reading through this same query layer."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW SCHEMAS IN iceberg"
```
EXPECTED OUTPUT:
```text
bronze
silver
gold
```
EXPLAIN: "This shows the medallion layers are visible through one query engine."

RUN:
```bash
docker exec lakehouse_trino trino --server http://localhost:8080 --catalog iceberg --execute "SELECT machine_id, production_date, oee_score FROM gold.gold_manufacturing_oee_mart ORDER BY production_date DESC LIMIT 5"
```
EXPECTED OUTPUT: five gold OEE rows.  
EXPLAIN: "This is the same gold data the BI tools will use next."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
curl -s http://localhost:8180/v1/info | python3 -m json.tool
```
Expected response: JSON with `nodeVersion`.

Check 2:
```bash
docker exec lakehouse_trino trino --server http://localhost:8080 --execute "SHOW CATALOGS"
```
Expected response: includes `iceberg`.

Check 3:
```bash
docker exec lakehouse_trino trino --server http://localhost:8080 --catalog iceberg --execute "SHOW TABLES IN gold"
```
Expected response: lists the gold marts.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data is still physically stored as Iceberg tables on SeaweedFS, but it is now logically exposed as SQL relations in the `iceberg` catalog. Apache Superset is the next tool that picks it up, using a Trino SQLAlchemy URI to read gold marts for dashboards. The handoff happens technically through Trino’s HTTP SQL interface and metadata discovery of the gold schema. Before the handoff, the data is a table in the lakehouse; after the handoff, it becomes a dashboard dataset and chart source inside Superset.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“Now that the marts are queryable through SQL, let’s open the BI layer and show how those business tables become client-facing dashboards.”

═══════════════════════════════════════════════
LAYER 9 — APACHE SUPERSET (BI Dashboard)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the final-mile problem of turning manufacturing, quality, inventory, and training data into business decisions. For a pharmaceutical manufacturer, Superset is where gold marts from MES, IQMS, Trackwise, SAP ECC, and TMS become an executive-ready dashboard for OEE, release quality, CAPA, and compliance conversations. This is the layer the client will immediately understand because it translates technical transformations into business metrics and trends. In regulated operations it is especially valuable because it keeps users on governed shared metrics instead of spreadsheet copies.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Runs Superset and wires it to PostgreSQL for metadata.

KEY SECTION:
```yaml
  superset:
    image: apache/superset:3.1.3
    container_name: lakehouse_superset
    profiles: ["analytics", "all"]
    environment:
      SUPERSET_SECRET_KEY: ${SUPERSET_SECRET_KEY:-supersecret123}
      DATABASE_URL: postgresql+psycopg2://admin:admin123@postgres:5432/superset
    volumes:
      - ./configs/superset/superset_config.py:/app/pythonpath/superset_config.py
    command: >
      bash -c "superset db upgrade &&
               superset fab create-admin --username admin ... --password admin &&
               superset init &&
               superset run -h 0.0.0.0 -p 8088"
```
LINE BY LINE:
- The image is Superset 3.1.3.
- Metadata is stored in PostgreSQL.
- The project mounts a custom config file.
- The command upgrades metadata, creates an admin account, initializes the app, and launches the web server.
WHY IT EXISTS: It makes the BI layer fully reproducible from Compose.

FILE: `configs/superset/superset_config.py`  
PURPOSE: Customizes Superset and predefines the Trino connection details.

KEY SECTION:
```python
ADDITIONAL_DATABASES = {
    "trino_iceberg": {
        "sqlalchemy_uri": "trino://admin@trino:8080/iceberg",
        "name": "Enterprise Lakehouse (Trino/Iceberg)",
    }
}
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DRILL_TO_DETAIL": True,
}
```
LINE BY LINE:
- `ADDITIONAL_DATABASES` documents the Trino target.
- The `sqlalchemy_uri` is the same engine the dbt and Trino layers use.
- The feature flags enable filters and drill-down behavior useful in demos.
WHY IT EXISTS: It shortens the distance from platform startup to usable dashboards.

FILE: `scripts/setup_superset.py`  
PURPOSE: Uses the Superset REST API to create the Trino database, datasets, and dashboard shell.

KEY SECTION:
```python
database_id = get_or_create_database(token, csrf_token)
datasets = {
    "gold_oee_dashboard": get_or_create_dataset(token, csrf_token, database_id, "gold_oee_dashboard"),
    "gold_batch_summary": get_or_create_dataset(token, csrf_token, database_id, "gold_batch_summary"),
    "gold_quality_kpis": get_or_create_dataset(token, csrf_token, database_id, "gold_quality_kpis"),
    "gold_production_efficiency": get_or_create_dataset(token, csrf_token, database_id, "gold_production_efficiency"),
}
dashboard_id = get_or_create_dashboard(token, csrf_token)
```
LINE BY LINE:
- The script authenticates to the Superset API.
- It creates or reuses the Trino database connection.
- It registers the gold marts as datasets.
- It creates or reuses a demo dashboard.
WHY IT EXISTS: It turns dashboard bootstrap into code instead of a manual click path.

FILE: `scripts/create_superset_charts.py`  
PURPOSE: Creates charts on top of the datasets through the REST API.

KEY SECTION:
```python
create_chart(token, csrf_token, dashboard_id, 3, "Total Production Batches", "big_number", {...})
create_chart(token, csrf_token, dashboard_id, 1, "Quality Rate by Product", "echarts_timeseries_bar", {...})
create_chart(token, csrf_token, dashboard_id, 1, "Average Process Temperature Trend", "echarts_timeseries_line", {...})
```
LINE BY LINE:
- Each `create_chart(...)` call posts a chart definition to Superset.
- The charts include a KPI card, a product quality bar chart, and a time-series temperature chart.
WHY IT EXISTS: It demonstrates that the BI layer can be provisioned as code, not just designed manually.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` block:
```yaml
  superset:
    image: apache/superset:3.1.3
    container_name: lakehouse_superset
    profiles: ["analytics", "all"]
```

Start command:
```bash
make up-analytics
```

Direct compose alternative:
```bash
docker compose --profile analytics up -d
```

Verify commands:
```bash
docker compose ps | grep superset
curl -s http://localhost:8500/health
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `configs/superset/superset_config.py`  
HIGHLIGHT LINES: `ADDITIONAL_DATABASES` and `FEATURE_FLAGS`  
SAY TO CLIENT: "This is how the BI layer is pointed at the governed Trino lakehouse and given the dashboard behaviors we want during the demo."

OPEN FILE: `scripts/setup_superset.py`  
HIGHLIGHT LINES: the `get_or_create_database`, `get_or_create_dataset`, and `get_or_create_dashboard` calls  
SAY TO CLIENT: "We provision the lakehouse connection and the dashboard datasets through the API so the BI layer is repeatable and not dependent on manual UI setup."

OPEN FILE: `scripts/create_superset_charts.py`  
HIGHLIGHT LINES: the chart creation calls  
SAY TO CLIENT: "These API calls define the KPI card and trend charts that turn our gold marts into a business-facing view."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:8500`  
NAVIGATE: log in with `admin / admin` → `Dashboards` → `Manufacturing Lakehouse - Live Demo`  
POINT TO: the big-number production total, quality chart, temperature trend, and any table visual on the dashboard  
SAY TO CLIENT: "This is the final business view. Everything on this page is reading from governed gold marts through Trino, so production, quality, and compliance are all tied back to the same underlying lakehouse."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
python scripts/setup_superset.py
```
EXPECTED OUTPUT:
```text
Superset setup complete.
Database ID: ...
Dataset IDs: ...
Dashboard ID: ...
Dashboard URL: http://localhost:8088/superset/dashboard/.../
```
EXPLAIN: "This shows the BI layer can be provisioned from code, including the lakehouse connection and the dashboard shell."

RUN:
```bash
curl -s http://localhost:8500/health
```
EXPECTED OUTPUT:
```text
OK
```
EXPLAIN: "This confirms the BI service itself is healthy before we present the charts."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
curl -s http://localhost:8500/health
```
Expected response:
```text
OK
```

Check 2:
```bash
python scripts/setup_superset.py
```
Expected response: database, dataset, and dashboard IDs print without HTTP errors.

Check 3:
```bash
curl -s -u admin:admin http://localhost:8500/api/v1/dashboard/
```
Expected response: JSON dashboard list.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
At the end of this layer, the data is no longer changing format; it is being presented as dashboard datasets, charts, and filters on top of the gold marts already served by Trino. The next layer, Grafana plus Prometheus, does not transform the business tables further, but it provides the operational health view that tells the client whether the platform running those dashboards is healthy. The handoff is technical rather than data-conversion based: Superset proves business consumption is working, and Grafana proves the platform supporting that consumption is healthy. Before the handoff, the client is looking at business KPIs; after it, the client is looking at service reliability, throughput, and pipeline health.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“We’ve shown the business dashboard layer, so the last thing to close with is the operational health layer that tells us the whole platform behind it is stable.”

═══════════════════════════════════════════════
LAYER 10 — GRAFANA + PROMETHEUS (Platform Health)
═══════════════════════════════════════════════

─────────────────────────────────────────
BUSINESS CONTEXT
─────────────────────────────────────────
This layer solves the platform-health problem around the pipeline itself rather than the business data inside it. For a pharmaceutical manufacturer, Prometheus and Grafana provide operational visibility into whether the ingestion, transformation, storage, and serving stack is healthy enough to trust the manufacturing and quality views sourced from MES, IQMS, Trackwise, SAP ECC, and TMS data. That matters in regulated operations because data timeliness and platform reliability affect whether downstream reports can be used confidently. This is also the layer that helps operations teams identify service failure before it becomes a business-data or compliance issue.

─────────────────────────────────────────
CODE EXPLANATION
─────────────────────────────────────────
FILE: `docker-compose.yml`  
PURPOSE: Starts Prometheus, Grafana, Loki, Promtail, and exporters.

KEY SECTION:
```yaml
  prometheus:
    image: prom/prometheus:v2.51.2
  grafana:
    image: grafana/grafana:10.4.2
    environment:
      GF_SECURITY_ADMIN_USER: ${GRAFANA_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASS:-admin123}
      GF_INSTALL_PLUGINS: "trino-datasource"
  loki:
    image: grafana/loki:2.9.0
  promtail:
    image: grafana/promtail:2.9.0
  node-exporter:
    image: prom/node-exporter:v1.7.0
  postgres-exporter:
    image: prometheuscommunity/postgres-exporter:v0.15.0
```
LINE BY LINE:
- `prometheus` stores metrics.
- `grafana` visualizes metrics and logs and installs the Trino plugin.
- `loki` stores logs.
- `promtail` ships logs into Loki.
- The exporters expose host and PostgreSQL metrics to Prometheus.
WHY IT EXISTS: It gives the demo an operational observability layer across the full stack.

FILE: `configs/prometheus/prometheus.yml`  
PURPOSE: Defines the metric scrape targets.

KEY SECTION:
```yaml
scrape_configs:
  - job_name: spark-master
    static_configs:
      - targets: ["spark-master:8181"]
    metrics_path: /metrics/prometheus

  - job_name: airflow
    static_configs:
      - targets: ["airflow-webserver:8080"]
    metrics_path: /admin/metrics

  - job_name: trino
    static_configs:
      - targets: ["trino:8080"]
    metrics_path: /v1/jmx/mbean/trino.execution%3Aname%3DQueryManager
```
LINE BY LINE:
- Each `job_name` creates a scrape job.
- `targets` tells Prometheus which container and port to poll.
- `metrics_path` points to the service-specific metrics endpoint.
WHY IT EXISTS: It makes the health story measurable rather than anecdotal.

FILE: `configs/grafana/provisioning/datasources/datasources.yml`  
PURPOSE: Pre-provisions Grafana data sources.

KEY SECTION:
```yaml
datasources:
  - name: Prometheus
    uid: tpl-prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
  - name: Loki
    uid: tpl-loki
    type: loki
    url: http://loki:3100
  - name: Trino
    uid: tpl-trino
    type: trino-datasource
    url: http://trino:8080
```
LINE BY LINE:
- Prometheus is the default metrics source.
- Loki provides log search.
- Trino gives Grafana access to lakehouse tables for mixed dashboards.
WHY IT EXISTS: The dashboards are usable immediately after startup.

FILE: `configs/grafana/provisioning/dashboards/dashboards.yml`  
PURPOSE: Tells Grafana where to load dashboard JSON files from.

KEY SECTION:
```yaml
providers:
  - name: "TPL Lakehouse Dashboards"
    folder: "TPL Lakehouse"
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards
```
LINE BY LINE:
- The provider creates a folder in Grafana.
- The JSON dashboards are loaded from the mounted filesystem path.
WHY IT EXISTS: It keeps dashboard provisioning in code and under version control.

FILE: `configs/grafana/provisioning/dashboards/data_pipeline_health.json`  
PURPOSE: Defines the platform health dashboard.

KEY SECTION:
```json
{
  "title": "Data Pipeline Health"
}
```
Key panels discovered from the file:
```text
Kafka Pipeline Events by Source
Airflow DAG Success Rate (24h)
Pipeline Task Duration (Avg 24h)
SeaweedFS Lakehouse Storage
Bronze / Silver / Gold Row Counts
Failed Airflow Tasks (Last 24h)
```
WHY IT EXISTS: It gives the demo a single health dashboard for the data pipeline.

FILE: `configs/grafana/provisioning/dashboards/manufacturing_oee.json`  
PURPOSE: Defines an operations-oriented OEE dashboard.

KEY SECTION:
```text
OEE Score
Availability / Performance / Quality (7-Day Trend)
Production Volume by Machine
Top 5 Machines by Scrap Percentage
Real-Time Machine Status
```
WHY IT EXISTS: It turns platform and business signals into one operations monitoring surface.

FILE: `configs/grafana/provisioning/dashboards/compliance_audit.json`  
PURPOSE: Defines a compliance and audit dashboard.

KEY SECTION:
```text
Open CAPAs by Department
CAPA RAG Status Distribution
Quality Test Pass Rate Trend
Overdue Training Count
Recent Audit Log Entries
```
WHY IT EXISTS: It lets the client see that compliance monitoring sits alongside platform monitoring.

FILE: `configs/loki/loki-config.yml`  
PURPOSE: Configures Loki log storage.

KEY SECTION:
```yaml
server:
  http_listen_port: 3100
storage_config:
  filesystem:
    directory: /loki/chunks
```
WHY IT EXISTS: It gives Grafana a backing log store for operational troubleshooting.

FILE: `configs/promtail/promtail-config.yml`  
PURPOSE: Configures log shipping from Docker to Loki.

KEY SECTION:
```yaml
scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
    relabel_configs:
      - source_labels: [__meta_docker_container_label_com_docker_compose_service]
        target_label: service
```
LINE BY LINE:
- Docker service discovery watches running containers.
- Relabeling attaches the compose service name to each log stream.
WHY IT EXISTS: It makes log streams filterable by service in Grafana.

─────────────────────────────────────────
DOCKER COMMAND THAT STARTS THIS SERVICE
─────────────────────────────────────────
Exact `docker-compose.yml` blocks:
```yaml
  prometheus: { ... }
  grafana: { ... }
  loki: { ... }
  promtail: { ... }
  node-exporter: { ... }
  postgres-exporter: { ... }
```

Start command:
```bash
make up-monitoring
```

Direct compose alternative:
```bash
docker compose --profile monitoring up -d
```

Verify commands:
```bash
docker compose ps | grep grafana
curl -s http://localhost:9090/-/healthy
curl -s http://localhost:3000/api/health
```

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — EDITOR VIEW
─────────────────────────────────────────
OPEN FILE: `configs/prometheus/prometheus.yml`  
HIGHLIGHT LINES: the `spark-master`, `airflow`, `trino`, `seaweedfs`, and `postgres` scrape jobs  
SAY TO CLIENT: "This is the monitoring fabric collecting health and performance data across the ingestion, processing, storage, and query layers."

OPEN FILE: `configs/grafana/provisioning/datasources/datasources.yml`  
HIGHLIGHT LINES: the Prometheus, Loki, and Trino datasource definitions  
SAY TO CLIENT: "Grafana is pre-wired to metrics, logs, and SQL so the operations view can combine platform and business context."

OPEN FILE: `configs/promtail/promtail-config.yml`  
HIGHLIGHT LINES: the Docker service discovery block  
SAY TO CLIENT: "This is how logs from the running containers become searchable in Grafana by service name."

─────────────────────────────────────────
WHAT TO SHOW THE CLIENT — BROWSER VIEW
─────────────────────────────────────────
OPEN URL: `http://localhost:9090`  
NAVIGATE: `Status` → `Targets`  
POINT TO: active targets and their `UP` health state  
SAY TO CLIENT: "This is the raw metrics control plane. If these targets are up, the observability layer is successfully collecting health data from the stack."

OPEN URL: `http://localhost:3000`  
NAVIGATE: log in with `admin / admin123` → `Dashboards` → `TPL Lakehouse` → `Data Pipeline Health`  
POINT TO: panels for Airflow success, row counts, storage, and failed tasks  
SAY TO CLIENT: "This is the operational dashboard for the platform itself. It tells us whether the pipeline is healthy enough for the business dashboards to be trusted."

OPEN URL: `http://localhost:3000`  
NAVIGATE: `Dashboards` → `TPL Lakehouse` → `Manufacturing OEE Dashboard`  
POINT TO: `OEE Score` and `Real-Time Machine Status`  
SAY TO CLIENT: "Here the observability layer starts blending platform telemetry with manufacturing outcome signals, which is useful for operations and site leadership."

─────────────────────────────────────────
LIVE COMMAND TO RUN DURING DEMO
─────────────────────────────────────────
RUN:
```bash
curl -s http://localhost:9090/api/v1/targets
```
EXPECTED OUTPUT:
```json
{"status":"success","data":{"activeTargets":[...]}}
```
EXPLAIN: "This shows Prometheus is actively scraping the services we rely on."

RUN:
```bash
curl -s http://localhost:3000/api/health
```
EXPECTED OUTPUT:
```json
{"database":"ok",...}
```
EXPLAIN: "Grafana itself is healthy and ready to render the monitoring dashboards."

─────────────────────────────────────────
HOW TO CONFIRM IT IS WORKING
─────────────────────────────────────────
Check 1:
```bash
curl -s http://localhost:9090/-/healthy
```
Expected response:
```text
Prometheus is Healthy.
```

Check 2:
```bash
curl -s http://localhost:3000/api/health
```
Expected response: JSON with `"database":"ok"`.

Check 3:
```bash
curl -s http://localhost:3100/ready
```
Expected response:
```text
ready
```

The monitoring demo can continue only if Prometheus, Grafana, and Loki all return healthy responses and Prometheus shows active targets.

─────────────────────────────────────────
DATA MOVEMENT TO NEXT LAYER
─────────────────────────────────────────
This is the final layer in the requested walkthrough, so there is no downstream transformation handoff after it. What this layer does provide is the operational evidence that every earlier handoff is healthy: Kafka is scraping, Airflow is running, Spark is reachable, Trino is queryable, SeaweedFS is available, and logs are searchable. In demo terms, this is the layer that validates the entire end-to-end story from synthetic event creation through business dashboards. Before this layer, each component is shown in its own context; after this layer, the client has a complete operating view of the whole platform.

─────────────────────────────────────────
TRANSITION PHRASE
─────────────────────────────────────────
“That closes the walkthrough: we started with generated plant data, moved it through ingestion, storage, transformation, orchestration, and analytics, and we’re ending with the operational view that proves the whole lakehouse is healthy.”
