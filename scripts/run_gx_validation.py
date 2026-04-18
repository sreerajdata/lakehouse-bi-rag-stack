from __future__ import annotations

import os
import shutil
from pathlib import Path

import great_expectations as gx
from great_expectations.data_context import FileDataContext
from sqlalchemy import create_engine, text


ROOT_DIR = Path(__file__).resolve().parents[1]
GX_DIR = ROOT_DIR / "great_expectations"
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "silver")
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_URL = f"trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}/{TRINO_SCHEMA}"


def ensure_context():
    if not GX_DIR.exists():
        FileDataContext.create(project_root_dir=str(ROOT_DIR))
    return gx.get_context(context_root_dir=str(GX_DIR))


def ensure_datasource(context):
    return context.sources.add_or_update_sql(
        name="trino_sql",
        connection_string=TRINO_URL,
    )


def fetch_sample_rows(engine, sql: str):
    with engine.connect() as connection:
        result = connection.execute(text(sql))
        return [dict(row._mapping) for row in result.fetchall()]


def failure_query(table_name: str, expectation_type: str, kwargs: dict):
    column = kwargs.get("column")
    if expectation_type == "expect_column_values_to_not_be_null":
        return f"select * from {table_name} where {column} is null limit 5"
    if expectation_type == "expect_column_values_to_be_in_set":
        allowed = ", ".join(f"'{value}'" for value in kwargs["value_set"])
        return f"select * from {table_name} where {column} is null or {column} not in ({allowed}) limit 5"
    if expectation_type == "expect_column_values_to_be_between":
        min_value = kwargs.get("min_value", 0)
        max_value = kwargs.get("max_value", 0)
        return (
            f"select * from {table_name} "
            f"where {column} is null or {column} < {min_value} or {column} > {max_value} limit 5"
        )
    if expectation_type == "expect_column_values_to_be_unique":
        return (
            f"select * from {table_name} "
            f"where {column} in (select {column} from {table_name} group by 1 having count(*) > 1) limit 5"
        )
    if expectation_type == "expect_table_row_count_to_be_between":
        return f"select count(*) as row_count from {table_name}"
    return f"select * from {table_name} limit 5"


def validate_asset(context, engine, datasource, suite_name: str, asset_name: str, table_name: str, expectations: list[dict]):
    existing_assets = set(datasource.get_asset_names())
    if asset_name in existing_assets:
        datasource.delete_asset(asset_name)

    asset = datasource.add_table_asset(
        name=asset_name,
        table_name=table_name,
        schema_name=TRINO_SCHEMA,
    )
    batch_request = asset.build_batch_request()
    if suite_name in context.list_expectation_suite_names():
        context.delete_expectation_suite(expectation_suite_name=suite_name)
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    results = []
    for expectation in expectations:
        expectation_type = expectation["type"]
        kwargs = expectation["kwargs"]
        group = expectation["group"]
        method = getattr(validator, expectation_type)
        result = method(**kwargs)
        results.append({"type": expectation_type, "kwargs": kwargs, "group": group, "success": result.success})

    validator.save_expectation_suite(discard_failed_expectations=False)
    checkpoint = context.add_or_update_checkpoint(name=suite_name, validator=validator)
    checkpoint_result = checkpoint.run()
    context.build_data_docs()

    grouped_results = {}
    for result in results:
        group = result["group"]
        grouped_results[group] = grouped_results.get(group, True) and result["success"]

    passed = sum(1 for success in grouped_results.values() if success)
    total = len(grouped_results)
    status_icon = "✅" if checkpoint_result.success else "❌"
    print(f"{status_icon} Checkpoint {suite_name}: {passed}/{total} expectations passed")

    if not checkpoint_result.success:
        full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}"
        for result in results:
            if result["success"]:
                continue
            query = failure_query(full_table_name, result["type"], result["kwargs"])
            samples = fetch_sample_rows(engine, query)
            print(
                f"FAILED {suite_name}::{result['type']} "
                f"column={result['kwargs'].get('column', 'table')}"
            )
            print(f"Sample rows: {samples}")

    return checkpoint_result.success


def main():
    context = ensure_context()
    datasource = ensure_datasource(context)
    engine = create_engine(TRINO_URL)

    mes_expectations = [
        {"group": "not_null", "type": "expect_column_values_to_not_be_null", "kwargs": {"column": "event_id"}},
        {"group": "not_null", "type": "expect_column_values_to_not_be_null", "kwargs": {"column": "batch_id"}},
        {"group": "not_null", "type": "expect_column_values_to_not_be_null", "kwargs": {"column": "event_ts"}},
        {"group": "status_set", "type": "expect_column_values_to_be_in_set", "kwargs": {"column": "status", "value_set": ["PASS", "FAIL", "WARNING"]}},
        {"group": "parameter_range", "type": "expect_column_values_to_be_between", "kwargs": {"column": "parameter_value", "min_value": 0, "max_value": 1000}},
        {"group": "row_count", "type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 400, "max_value": 600}},
        {"group": "event_id_unique", "type": "expect_column_values_to_be_unique", "kwargs": {"column": "event_id"}},
    ]
    quality_expectations = [
        {"group": "deviation_id_not_null", "type": "expect_column_values_to_not_be_null", "kwargs": {"column": "deviation_id"}},
        {"group": "batch_id_not_null", "type": "expect_column_values_to_not_be_null", "kwargs": {"column": "batch_id"}},
        {"group": "severity_set", "type": "expect_column_values_to_be_in_set", "kwargs": {"column": "severity", "value_set": ["CRITICAL", "MAJOR", "MINOR"]}},
        {"group": "severity_score_range", "type": "expect_column_values_to_be_between", "kwargs": {"column": "severity_score", "min_value": 1, "max_value": 3}},
    ]

    mes_ok = validate_asset(
        context=context,
        engine=engine,
        datasource=datasource,
        suite_name="bronze_mes_events",
        asset_name="silver_mes_events_asset",
        table_name="silver_mes_events",
        expectations=mes_expectations,
    )
    quality_ok = validate_asset(
        context=context,
        engine=engine,
        datasource=datasource,
        suite_name="silver_quality_events",
        asset_name="silver_quality_events_asset",
        table_name="silver_quality_events",
        expectations=quality_expectations,
    )

    docs_index = GX_DIR / "uncommitted" / "data_docs" / "local_site" / "index.html"
    if docs_index.exists():
        target_dir = GX_DIR / "data_docs"
        source_dir = docs_index.parent
        if target_dir.exists():
            shutil.rmtree(target_dir)
        shutil.copytree(source_dir, target_dir)

    if not (mes_ok and quality_ok):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
