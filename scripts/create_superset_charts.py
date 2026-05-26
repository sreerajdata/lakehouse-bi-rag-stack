from __future__ import annotations
import json
import os
import requests
from typing import Any

SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://superset:8088")
USERNAME = "admin"
PASSWORD = "admin"
SESSION = requests.Session()

def api(method: str, path: str, token: str | None = None, csrf_token: str | None = None, json_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if token: headers["Authorization"] = f"Bearer {token}"
    if csrf_token and method.upper() in {"POST", "PUT"}:
        headers["X-CSRFToken"] = csrf_token
        headers["Referer"] = SUPERSET_BASE_URL
    response = SESSION.request(method=method, url=f"{SUPERSET_BASE_URL}{path}", headers=headers, json=json_payload, timeout=30)
    response.raise_for_status()
    return response.json() if response.text else {}

def login() -> str:
    data = api("POST", "/api/v1/security/login", json_payload={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True})
    return data["access_token"]

def get_csrf_token(token: str) -> str:
    return api("GET", "/api/v1/security/csrf_token/", token=token)["result"]

def create_chart(token: str, csrf_token: str, dashboard_id: int, dataset_id: int, name: str, viz_type: str, params: dict):
    payload = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "dashboards": [dashboard_id]
    }
    return api("POST", "/api/v1/chart/", token=token, csrf_token=csrf_token, json_payload=payload)

def update_dataset_time_column(token: str, csrf_token: str, dataset_id: int, column_name: str):
    payload = {"main_dttm_col": column_name}
    return api("PUT", f"/api/v1/dataset/{dataset_id}", token=token, csrf_token=csrf_token, json_payload=payload)

def main():
    token = login()
    csrf_token = get_csrf_token(token)
    
    update_dataset_time_column(token, csrf_token, 1, "hour_window")
    update_dataset_time_column(token, csrf_token, 3, "report_date")

    dashboard_id = 1
    
    create_chart(token, csrf_token, dashboard_id, 3, "Total Production Batches", "big_number", {
        "metric": {"expressionType": "SIMPLE", "column": {"column_name": "total_batches"}, "aggregate": "SUM", "label": "Total Batches"},
        "granularity_sqla": "report_date",
        "time_range": "No filter",
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "y_axis_format": "SMART_NUMBER"
    })
    
    create_chart(token, csrf_token, dashboard_id, 1, "Quality Rate by Product", "echarts_timeseries_bar", {
        "groupby": ["product_code"],
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "quality_rate"}, "aggregate": "AVG", "label": "Avg Quality Rate"}],
        "granularity_sqla": "hour_window",
        "time_range": "No filter",
        "y_axis_format": ".2f"
    })
    
    create_chart(token, csrf_token, dashboard_id, 1, "Average Process Temperature Trend", "echarts_timeseries_line", {
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "avg_temp"}, "aggregate": "AVG", "label": "Avg Temp"}],
        "groupby": [],
        "granularity_sqla": "hour_window",
        "time_range": "No filter",
        "seriesType": "line",
        "show_value": True
    })

    create_chart(token, csrf_token, dashboard_id, 3, "Batch Release Status Distribution", "pie", {
        "groupby": ["product_code"],
        "metric": {"expressionType": "SIMPLE", "column": {"column_name": "released_batches"}, "aggregate": "SUM", "label": "Released Batches"},
        "granularity_sqla": "report_date",
        "time_range": "No filter",
        "pie_label_type": "percent",
        "donut": True
    })

    create_chart(token, csrf_token, dashboard_id, 2, "Detailed Production Data Table", "table", {
        "groupby": ["batch_id", "product_code", "machine_id", "start_time", "end_time", "actual_qty", "yield_pct", "status"],
        "metrics": [],
        "all_columns": ["batch_id", "product_code", "machine_id", "start_time", "end_time", "actual_qty", "yield_pct", "status"],
        "percent_metrics": [],
        "order_by_cols": [["start_time", False]],
        "table_timestamp_format": "smart_date",
        "page_size": 10
    })

    print("Successfully updated datasets and added 5 charts (including a data table).")

if __name__ == "__main__":
    main()
