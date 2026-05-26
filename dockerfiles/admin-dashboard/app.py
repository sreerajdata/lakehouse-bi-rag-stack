"""
Admin Ops Dashboard (Streamlit)
Provides real-time operational visibility into the data lakehouse stack.
"""

import json
import os
from datetime import datetime

import streamlit as st
import requests

AIRFLOW_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
SEAWEEDFS_URL = os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs-s3:8333")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")


def get_airflow_dags():
    """Fetch Airflow DAG statuses."""
    try:
        resp = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags",
            auth=("admin", "admin"),
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("dags", [])
    except Exception as e:
        st.error(f"Airflow connection failed: {e}")
    return []


def get_airflow_dag_runs(dag_id: str, limit: int = 5):
    """Fetch recent DAG runs."""
    try:
        resp = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=("admin", "admin"),
            params={"limit": limit, "order_by": "-execution_date"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("dag_runs", [])
    except Exception:
        pass
    return []


def get_seaweedfs_status():
    """Check SeaweedFS cluster status."""
    try:
        resp = requests.get("http://seaweedfs-master:9333/cluster/status", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def get_trino_info():
    """Fetch Trino cluster info."""
    try:
        resp = requests.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def trigger_dag(dag_id: str):
    """Trigger an Airflow DAG run."""
    try:
        resp = requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=("admin", "admin"),
            json={"conf": {}},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


st.set_page_config(
    page_title="Lakehouse Admin",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏭 Data Lakehouse — Admin Console")
st.caption(f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

with st.sidebar:
    st.header("🔧 Quick Actions")

    if st.button("🔄 Trigger Medallion Pipeline"):
        if trigger_dag("medallion_pipeline"):
            st.success("DAG triggered!")
        else:
            st.error("Failed to trigger DAG")

    if st.button("📄 Trigger PDF Processing"):
        if trigger_dag("pdf_processing_pipeline"):
            st.success("PDF pipeline triggered!")
        else:
            st.error("Failed to trigger")

    if st.button("🧹 Trigger Iceberg Maintenance"):
        if trigger_dag("iceberg_maintenance"):
            st.success("Maintenance triggered!")
        else:
            st.error("Failed to trigger")

    st.divider()
    st.header("🔗 Service Links")
    st.markdown(f"- [Grafana]({GRAFANA_URL})")
    st.markdown(f"- [Airflow]({AIRFLOW_URL})")
    st.markdown(f"- [Kafka UI](http://localhost:9000)")
    st.markdown(f"- [Trino UI](http://localhost:8180)")
    st.markdown(f"- [Spark UI](http://localhost:8181)")

col1, col2, col3, col4 = st.columns(4)

trino_info = get_trino_info()
with col1:
    if trino_info:
        st.metric("Trino", "🟢 Running",
                   f"v{trino_info.get('nodeVersion', {}).get('version', '?')}")
    else:
        st.metric("Trino", "🔴 Down", None)

swfs = get_seaweedfs_status()
with col2:
    if swfs:
        st.metric("SeaweedFS", "🟢 Running",
                   f"{len(swfs.get('Peers', []))+1} nodes")
    else:
        st.metric("SeaweedFS", "🔴 Down", None)

dags = get_airflow_dags()
with col3:
    active = len([d for d in dags if not d.get("is_paused")])
    st.metric("Airflow DAGs", f"{active} active", f"{len(dags)} total")

with col4:
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BOOTSTRAP],
                                consumer_timeout_ms=3000)
        topics = consumer.topics()
        consumer.close()
        st.metric("Kafka", "🟢 Running", f"{len(topics)} topics")
    except Exception:
        st.metric("Kafka", "🔴 Down", None)

st.divider()
st.header("📊 Pipeline Status")

if dags:
    for dag in dags[:10]:
        dag_id = dag["dag_id"]
        is_paused = dag.get("is_paused", True)
        status_icon = "⏸️" if is_paused else "▶️"

        with st.expander(f"{status_icon} {dag_id}", expanded=False):
            runs = get_airflow_dag_runs(dag_id)
            if runs:
                for run in runs[:5]:
                    state = run.get("state", "unknown")
                    state_icon = {
                        "success": "✅", "failed": "❌",
                        "running": "🔄", "queued": "⏳",
                    }.get(state, "❓")
                    st.write(
                        f"{state_icon} {state.upper()} — "
                        f"{run.get('execution_date', '?')[:19]}"
                    )
            else:
                st.write("No recent runs")
else:
    st.info("Could not connect to Airflow")

st.divider()
st.header("🕐 Data Freshness")
st.info("Data freshness metrics require active Trino connection with populated tables.")

st.divider()
st.caption(
    "Data Lakehouse Admin Dashboard v1.0 | "
    "Built with Streamlit | "
    "Compliance-Ready Infrastructure"
)
