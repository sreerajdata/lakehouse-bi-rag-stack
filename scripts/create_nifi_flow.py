from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


BASE_URL = os.getenv("NIFI_BASE_URL", "http://localhost:8090/nifi-api")
ROOT_DIR = Path(__file__).resolve().parents[1]


def _detect_root_pg_id() -> str:
    """Auto-detect the root process group ID from the NiFi API."""
    hardcoded = os.getenv("NIFI_ROOT_PG_ID", "")
    if hardcoded:
        return hardcoded
    try:
        data = api_request("GET", "/flow/process-groups/root")
        return data["processGroupFlow"]["id"]
    except Exception:
        return "root"


def api_request(method: str, path: str, payload: dict | None = None) -> dict | list | None:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(f"{BASE_URL}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode("utf-8")
            if not raw:
                return None
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: HTTP {exc.code} {body}") from exc


def get_flow(process_group_id: str) -> dict:
    return api_request("GET", f"/flow/process-groups/{process_group_id}")


def get_process_group_controller_services(process_group_id: str) -> list[dict]:
    data = api_request("GET", f"/flow/process-groups/{process_group_id}/controller-services")
    return data.get("controllerServices", [])


def create_process_group(parent_id: str, name: str, x: float, y: float) -> dict:
    payload = {
        "revision": {"version": 0},
        "component": {"name": name, "position": {"x": x, "y": y}},
    }
    return api_request("POST", f"/process-groups/{parent_id}/process-groups", payload)


def create_processor(process_group_id: str, name: str, processor_type: str, bundle_artifact: str, x: float, y: float) -> dict:
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": processor_type,
            "bundle": {"group": "org.apache.nifi", "artifact": bundle_artifact, "version": "1.23.2"},
            "position": {"x": x, "y": y},
        },
    }
    return api_request("POST", f"/process-groups/{process_group_id}/processors", payload)


def update_processor(processor_id: str, properties: dict | None = None, auto_terminated: list[str] | None = None) -> dict:
    processor = api_request("GET", f"/processors/{processor_id}")
    component = processor["component"]
    config = component["config"]
    config["properties"] = config.get("properties", {})
    if properties:
        config["properties"].update(properties)
    if auto_terminated is not None:
        config["autoTerminatedRelationships"] = auto_terminated

    payload = {
        "revision": {"version": processor["revision"]["version"]},
        "component": {
            "id": processor_id,
            "name": component["name"],
            "config": {"properties": config["properties"], "autoTerminatedRelationships": config.get("autoTerminatedRelationships", [])},
        },
    }
    return api_request("PUT", f"/processors/{processor_id}", payload)


def create_controller_service(process_group_id: str, name: str, service_type: str) -> dict:
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": service_type,
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-record-serialization-services-nar",
                "version": "1.23.2",
            },
        },
    }
    return api_request("POST", f"/process-groups/{process_group_id}/controller-services", payload)


def update_controller_service(service_id: str, properties: dict) -> dict:
    service = api_request("GET", f"/controller-services/{service_id}")
    component = service["component"]
    current = component.get("properties", {})
    current.update(properties)
    payload = {
        "revision": {"version": service["revision"]["version"]},
        "component": {"id": service_id, "name": component["name"], "properties": current},
    }
    return api_request("PUT", f"/controller-services/{service_id}", payload)


def enable_controller_service(service_id: str) -> None:
    service = api_request("GET", f"/controller-services/{service_id}")
    payload = {"revision": {"version": service["revision"]["version"]}, "state": "ENABLED"}
    api_request("PUT", f"/controller-services/{service_id}/run-status", payload)

    for _ in range(30):
        refreshed = api_request("GET", f"/controller-services/{service_id}")
        state = refreshed["component"]["state"]
        if state == "ENABLED":
            return
        if state == "DISABLED":
            raise RuntimeError(f"Controller service {refreshed['component']['name']} did not enable")
        time.sleep(2)
    raise RuntimeError(f"Timed out enabling controller service {service_id}")


def disable_controller_service(service_id: str) -> None:
    service = api_request("GET", f"/controller-services/{service_id}")
    payload = {"revision": {"version": service["revision"]["version"]}, "state": "DISABLED"}
    api_request("PUT", f"/controller-services/{service_id}/run-status", payload)
    for _ in range(20):
        refreshed = api_request("GET", f"/controller-services/{service_id}")
        if refreshed["component"]["state"] == "DISABLED":
            return
        time.sleep(1)


def delete_controller_service(service_id: str) -> None:
    service = api_request("GET", f"/controller-services/{service_id}")
    if service["component"]["state"] != "DISABLED":
        try:
            disable_controller_service(service_id)
        except Exception:
            pass
    refreshed = api_request("GET", f"/controller-services/{service_id}")
    revision = refreshed["revision"]["version"]
    api_request("DELETE", f"/controller-services/{service_id}?version={revision}&clientId=codex", None)


def create_connection(process_group_id: str, source_id: str, destination_id: str, relationship: str) -> dict:
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": source_id, "groupId": process_group_id, "type": "PROCESSOR"},
            "destination": {"id": destination_id, "groupId": process_group_id, "type": "PROCESSOR"},
            "selectedRelationships": [relationship],
            "backPressureObjectThreshold": "10000",
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration": "0 sec",
        },
    }
    return api_request("POST", f"/process-groups/{process_group_id}/connections", payload)


def start_processor(processor_id: str) -> None:
    processor = api_request("GET", f"/processors/{processor_id}")
    validation_errors = processor["component"].get("validationErrors", [])
    if validation_errors:
        raise RuntimeError(f"Processor {processor['component']['name']} invalid: {validation_errors}")

    payload = {"revision": {"version": processor["revision"]["version"]}, "state": "RUNNING"}
    api_request("PUT", f"/processors/{processor_id}/run-status", payload)


def stop_processor(processor_id: str) -> None:
    processor = api_request("GET", f"/processors/{processor_id}")
    payload = {"revision": {"version": processor["revision"]["version"]}, "state": "STOPPED"}
    api_request("PUT", f"/processors/{processor_id}/run-status", payload)


def delete_processor(processor_id: str) -> None:
    processor = api_request("GET", f"/processors/{processor_id}")
    revision = processor["revision"]["version"]
    api_request("DELETE", f"/processors/{processor_id}?version={revision}&clientId=codex", None)


def delete_process_group(group_id: str) -> None:
    flow = get_flow(group_id)
    for processor in flow["processGroupFlow"]["flow"]["processors"]:
        try:
            stop_processor(processor["id"])
        except Exception:
            pass
    time.sleep(2)
    for connection in flow["processGroupFlow"]["flow"]["connections"]:
        delete_connection(connection["id"])
    for processor in flow["processGroupFlow"]["flow"]["processors"]:
        delete_processor(processor["id"])
    for service in get_process_group_controller_services(group_id):
        delete_controller_service(service["id"])

    group = api_request("GET", f"/process-groups/{group_id}")
    revision = group["revision"]["version"]
    api_request("DELETE", f"/process-groups/{group_id}?version={revision}&clientId=codex", None)


def delete_connection(connection_id: str) -> None:
    connection = api_request("GET", f"/connections/{connection_id}")
    drop_connection_queue(connection_id)
    refreshed = api_request("GET", f"/connections/{connection_id}")
    revision = refreshed["revision"]["version"]
    api_request("DELETE", f"/connections/{connection_id}?version={revision}&clientId=codex", None)


def drop_connection_queue(connection_id: str) -> None:
    response = api_request("POST", f"/flowfile-queues/{connection_id}/drop-requests", {"drop-request": {"id": connection_id}})
    drop_request = response.get("dropRequest", {})
    drop_request_id = drop_request.get("id")
    if not drop_request_id:
        return

    for _ in range(45):
        status = api_request("GET", f"/flowfile-queues/{connection_id}/drop-requests/{drop_request_id}")
        details = status.get("dropRequest", {})
        if details.get("finished"):
            break
        time.sleep(1)
    api_request("DELETE", f"/flowfile-queues/{connection_id}/drop-requests/{drop_request_id}")


def cleanup_root(root_pg_id: str) -> None:
    flow = get_flow(root_pg_id)

    for processor in flow["processGroupFlow"]["flow"]["processors"]:
        if processor["component"]["name"].startswith("tmp-probe-"):
            try:
                stop_processor(processor["id"])
            except Exception:
                pass
            delete_processor(processor["id"])

    for group in flow["processGroupFlow"]["flow"]["processGroups"]:
        group_name = group["component"]["name"]
        if group_name.startswith("FLOW 1 - CSV File Ingestion") or group_name.startswith("FLOW 2 - Kafka Consumer Flow"):
            delete_process_group(group["id"])
    for service in get_process_group_controller_services(root_pg_id):
        if service["component"]["name"].startswith("tmp-"):
            delete_controller_service(service["id"])


def verify_processors_running(process_group_id: str) -> None:
    flow = get_flow(process_group_id)
    for processor in flow["processGroupFlow"]["flow"]["processors"]:
        if processor["component"]["state"] != "RUNNING":
            raise RuntimeError(f"Processor {processor['component']['name']} is {processor['component']['state']}, not RUNNING")


def aws_ls(prefix: str) -> str:
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        "lakehouse-base-build_lakehouse_net",
        "-e",
        "AWS_ACCESS_KEY_ID=admin",
        "-e",
        "AWS_SECRET_ACCESS_KEY=admin123",
        "amazon/aws-cli:2.15.0",
        "--endpoint-url",
        "http://seaweedfs-s3:8333",
        "s3",
        "ls",
        prefix,
    ]
    result = subprocess.run(command, cwd=ROOT_DIR, capture_output=True, text=True, check=True)
    return result.stdout


def main() -> None:
    root_pg_id = _detect_root_pg_id()
    print(f"Using NiFi at {BASE_URL}, root PG = {root_pg_id}")
    if os.getenv("NIFI_CLEANUP_EXISTING", "true").lower() == "true":
        cleanup_root(root_pg_id)

    flow1 = create_process_group(root_pg_id, "FLOW 1 - CSV File Ingestion", 40.0, 80.0)
    flow2 = create_process_group(root_pg_id, "FLOW 2 - Kafka Consumer Flow", 40.0, 520.0)
    flow1_id = flow1["component"]["id"]
    flow2_id = flow2["component"]["id"]

    csv_reader = create_controller_service(flow1_id, "CSV Reader", "org.apache.nifi.csv.CSVReader")
    csv_writer = create_controller_service(flow1_id, "CSV Split Writer", "org.apache.nifi.csv.CSVRecordSetWriter")
    json_writer = create_controller_service(flow1_id, "JSON Writer", "org.apache.nifi.json.JsonRecordSetWriter")

    update_controller_service(csv_reader["id"], {"schema-access-strategy": "csv-header-derived", "CSV Format": "custom"})
    update_controller_service(csv_writer["id"], {"Include Header Line": "true"})
    update_controller_service(json_writer["id"], {"Schema Write Strategy": "no-schema", "Pretty Print JSON": "false"})

    for service_id in (csv_reader["id"], csv_writer["id"], json_writer["id"]):
        enable_controller_service(service_id)

    list_file = create_processor(flow1_id, "ListFile", "org.apache.nifi.processors.standard.ListFile", "nifi-standard-nar", 0.0, 0.0)
    fetch_file = create_processor(flow1_id, "FetchFile", "org.apache.nifi.processors.standard.FetchFile", "nifi-standard-nar", 260.0, 0.0)
    split_record = create_processor(flow1_id, "SplitRecord", "org.apache.nifi.processors.standard.SplitRecord", "nifi-standard-nar", 520.0, 0.0)
    stamp_source_name = create_processor(flow1_id, "StampSourceFilename", "org.apache.nifi.processors.attributes.UpdateAttribute", "nifi-update-attribute-nar", 780.0, 0.0)
    convert_record = create_processor(flow1_id, "ConvertRecord", "org.apache.nifi.processors.standard.ConvertRecord", "nifi-standard-nar", 1040.0, 0.0)
    publish_kafka = create_processor(flow1_id, "PublishKafka", "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6", "nifi-kafka-2-6-nar", 1300.0, 0.0)
    put_s3_ingest = create_processor(flow1_id, "PutS3Object", "org.apache.nifi.processors.aws.s3.PutS3Object", "nifi-aws-nar", 1560.0, 0.0)
    log_attribute = create_processor(flow1_id, "LogAttribute", "org.apache.nifi.processors.standard.LogAttribute", "nifi-standard-nar", 1820.0, 0.0)

    update_processor(
        list_file["id"],
        {
            "Input Directory": "/opt/nifi/data/source",
            "File Filter": ".*\\.csv",
            "Recurse Subdirectories": "false",
        },
        [],
    )
    update_processor(
        fetch_file["id"],
        {},
        ["failure", "not.found", "permission.denied"],
    )
    update_processor(
        split_record["id"],
        {"Record Reader": csv_reader["id"], "Record Writer": csv_writer["id"], "Records Per Split": "1"},
        ["failure", "original"],
    )
    update_processor(
        stamp_source_name["id"],
        {"source_filename": "${filename}"},
        ["failure"],
    )
    update_processor(
        convert_record["id"],
        {"record-reader": csv_reader["id"], "record-writer": json_writer["id"]},
        ["failure"],
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
            "Object Key": "nifi-ingest-v3/${source_filename}/${now():format(\"yyyyMMddHHmmssSSS\")}-${fragment.index}.json",
            "Endpoint Override URL": "http://seaweedfs-s3:8333",
            "Access Key": "admin",
            "Secret Key": "admin123",
            "Region": "us-east-1",
            "use-path-style-access": "true",
            "Content Type": "application/json",
        },
        ["failure"],
    )
    update_processor(log_attribute["id"], {}, ["success"])

    create_connection(flow1_id, list_file["id"], fetch_file["id"], "success")
    create_connection(flow1_id, fetch_file["id"], split_record["id"], "success")
    create_connection(flow1_id, split_record["id"], stamp_source_name["id"], "splits")
    create_connection(flow1_id, stamp_source_name["id"], convert_record["id"], "success")
    create_connection(flow1_id, convert_record["id"], publish_kafka["id"], "success")
    create_connection(flow1_id, publish_kafka["id"], put_s3_ingest["id"], "success")
    create_connection(flow1_id, put_s3_ingest["id"], log_attribute["id"], "success")

    consume_kafka = create_processor(flow2_id, "ConsumeKafka", "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6", "nifi-kafka-2-6-nar", 0.0, 0.0)
    evaluate_json = create_processor(flow2_id, "EvaluateJSON", "org.apache.nifi.processors.standard.EvaluateJsonPath", "nifi-standard-nar", 280.0, 0.0)
    route_on_attr = create_processor(flow2_id, "RouteOnAttribute", "org.apache.nifi.processors.standard.RouteOnAttribute", "nifi-standard-nar", 560.0, 0.0)
    put_s3_pass = create_processor(flow2_id, "PutS3Object PASS", "org.apache.nifi.processors.aws.s3.PutS3Object", "nifi-aws-nar", 840.0, -120.0)
    put_s3_fail = create_processor(flow2_id, "PutS3Object FAIL", "org.apache.nifi.processors.aws.s3.PutS3Object", "nifi-aws-nar", 840.0, 120.0)

    update_processor(
        consume_kafka["id"],
        {
            "bootstrap.servers": "kafka:9092",
            "topic": "raw.mes.events",
            "group.id": "nifi-mes-events-demo",
            "auto.offset.reset": "earliest",
            "Commit Offsets": "true",
        },
        [],
    )
    update_processor(
        evaluate_json["id"],
        {"Destination": "flowfile-attribute", "status": "$.status"},
        ["failure", "unmatched"],
    )
    update_processor(route_on_attr["id"], {"pass": "${status:equals('PASS')}"}, ["unmatched"])
    update_processor(
        put_s3_pass["id"],
        {
            "Bucket": "bronze",
            "Object Key": "mes/pass/${now():format(\"yyyyMMddHHmmssSSS\")}-${kafka.offset}.json",
            "Endpoint Override URL": "http://seaweedfs-s3:8333",
            "Access Key": "admin",
            "Secret Key": "admin123",
            "Region": "us-east-1",
            "use-path-style-access": "true",
            "Content Type": "application/json",
        },
        ["success", "failure"],
    )
    update_processor(
        put_s3_fail["id"],
        {
            "Bucket": "bronze",
            "Object Key": "mes/failures/${now():format(\"yyyyMMddHHmmssSSS\")}-${kafka.offset}.json",
            "Endpoint Override URL": "http://seaweedfs-s3:8333",
            "Access Key": "admin",
            "Secret Key": "admin123",
            "Region": "us-east-1",
            "use-path-style-access": "true",
            "Content Type": "application/json",
        },
        ["success", "failure"],
    )

    create_connection(flow2_id, consume_kafka["id"], evaluate_json["id"], "success")
    create_connection(flow2_id, evaluate_json["id"], route_on_attr["id"], "matched")
    create_connection(flow2_id, route_on_attr["id"], put_s3_pass["id"], "pass")
    create_connection(flow2_id, route_on_attr["id"], put_s3_fail["id"], "unmatched")

    for processor_id in [
        list_file["id"],
        fetch_file["id"],
        split_record["id"],
        stamp_source_name["id"],
        convert_record["id"],
        publish_kafka["id"],
        put_s3_ingest["id"],
        log_attribute["id"],
        consume_kafka["id"],
        evaluate_json["id"],
        route_on_attr["id"],
        put_s3_pass["id"],
        put_s3_fail["id"],
    ]:
        start_processor(processor_id)

    time.sleep(10)
    verify_processors_running(flow1_id)
    verify_processors_running(flow2_id)

    nifi_ingest_objects = aws_ls("s3://bronze/nifi-ingest-v3")
    if "nifi-ingest-v3/" not in nifi_ingest_objects:
        raise RuntimeError("No files found under s3://bronze/nifi-ingest-v3 after starting the flow")

    print("NiFi flow created successfully.")
    print("FLOW 1 processors are RUNNING.")
    print("FLOW 2 processors are RUNNING.")
    print("Objects found under s3://bronze/nifi-ingest-v3/:")
    print(nifi_ingest_objects)


if __name__ == "__main__":
    main()
