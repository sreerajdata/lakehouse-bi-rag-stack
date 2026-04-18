from __future__ import annotations

import json
import os
from typing import Any

import requests


SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://localhost:8088")
USERNAME = os.getenv("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
TRINO_URI = os.getenv("SUPERSET_TRINO_URI", "trino://admin@trino:8080/iceberg")
SESSION = requests.Session()


def api(
    method: str,
    path: str,
    token: str | None = None,
    csrf_token: str | None = None,
    json_payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if csrf_token and method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
        headers["X-CSRFToken"] = csrf_token
        headers["Referer"] = SUPERSET_BASE_URL
    response = SESSION.request(
        method=method,
        url=f"{SUPERSET_BASE_URL}{path}",
        headers=headers,
        json=json_payload,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    if response.text:
        return response.json()
    return {}


def login() -> str:
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True,
    }
    data = api("POST", "/api/v1/security/login", json_payload=payload)
    return data["access_token"]


def get_csrf_token(token: str) -> str:
    data = api("GET", "/api/v1/security/csrf_token/", token=token)
    return data["result"]


def get_or_create_database(token: str, csrf_token: str) -> int:
    payload = {
        "database_name": "Lakehouse (Trino)",
        "sqlalchemy_uri": TRINO_URI,
        "expose_in_sqllab": True,
    }
    try:
        created = api("POST", "/api/v1/database/", token=token, csrf_token=csrf_token, json_payload=payload)
        return int(created["id"])
    except requests.HTTPError:
        result = api(
            "GET",
            "/api/v1/database/",
            token=token,
            params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": "Lakehouse (Trino)"}]})},
        )
        return int(result["result"][0]["id"])


def get_or_create_dataset(token: str, csrf_token: str, database_id: int, table_name: str) -> int:
    payload = {
        "database": database_id,
        "schema": "gold",
        "table_name": table_name,
    }
    try:
        created = api("POST", "/api/v1/dataset/", token=token, csrf_token=csrf_token, json_payload=payload)
        return int(created["id"])
    except requests.HTTPError:
        result = api(
            "GET",
            "/api/v1/dataset/",
            token=token,
            params={
                "q": json.dumps(
                    {
                        "filters": [
                            {"col": "table_name", "opr": "eq", "value": table_name},
                            {"col": "schema", "opr": "eq", "value": "gold"},
                        ]
                    }
                )
            },
        )
        return int(result["result"][0]["id"])


def get_or_create_dashboard(token: str, csrf_token: str) -> int:
    title = "Manufacturing Lakehouse - Live Demo"
    payload = {"dashboard_title": title}
    try:
        created = api("POST", "/api/v1/dashboard/", token=token, csrf_token=csrf_token, json_payload=payload)
        return int(created["id"])
    except requests.HTTPError:
        result = api(
            "GET",
            "/api/v1/dashboard/",
            token=token,
            params={"q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": title}]})},
        )
        return int(result["result"][0]["id"])


def main() -> None:
    token = login()
    csrf_token = get_csrf_token(token)
    database_id = get_or_create_database(token, csrf_token)
    datasets = {
        "gold_oee_dashboard": get_or_create_dataset(token, csrf_token, database_id, "gold_oee_dashboard"),
        "gold_batch_summary": get_or_create_dataset(token, csrf_token, database_id, "gold_batch_summary"),
        "gold_quality_kpis": get_or_create_dataset(token, csrf_token, database_id, "gold_quality_kpis"),
        "gold_production_efficiency": get_or_create_dataset(token, csrf_token, database_id, "gold_production_efficiency"),
    }
    dashboard_id = get_or_create_dashboard(token, csrf_token)

    print("Superset setup complete.")
    print(f"Database ID: {database_id}")
    print(f"Dataset IDs: {datasets}")
    print(f"Dashboard ID: {dashboard_id}")
    print(f"Dashboard URL: {SUPERSET_BASE_URL}/superset/dashboard/{dashboard_id}/")


if __name__ == "__main__":
    main()
