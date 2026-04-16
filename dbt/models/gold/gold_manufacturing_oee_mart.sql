{{
  config(
    materialized='table',
    tags=['gold', 'oee', 'manufacturing']
  )
}}

/*
  Gold Layer: Manufacturing OEE & Production Performance Mart
  Powers: Superset dashboards, Trino direct queries, Power BI DirectQuery
  Grain: One row per machine per shift per day
*/

WITH production AS (
    SELECT
        machine_id,
        plant,
        shift,
        DATE(start_time)             AS production_date,
        _ingest_year                 AS year,
        _ingest_month                AS month,
        _ingest_day                  AS day,
        COUNT(*)                     AS total_orders,
        SUM(planned_qty)             AS total_planned_qty,
        SUM(actual_qty)              AS total_actual_qty,
        SUM(rejected_qty)            AS total_rejected_qty,
        AVG(yield_pct)               AS avg_yield_pct,
        AVG(scrap_pct)               AS avg_scrap_pct,
        SUM(duration_minutes)        AS total_production_minutes,
        COUNTIF(status = 'COMPLETED') AS completed_orders,
        COUNTIF(status = 'ON_HOLD')   AS on_hold_orders
    FROM {{ ref('silver_mes_production_orders') }}
    WHERE status IN ('COMPLETED', 'IN_PROGRESS', 'ON_HOLD')
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

quality AS (
    SELECT
        machine_id,
        DATE(tested_at)              AS quality_date,
        COUNT(*)                     AS total_tests,
        COUNTIF(result = 'PASS')     AS passed_tests,
        COUNTIF(result = 'FAIL')     AS failed_tests,
        ROUND(
            CAST(COUNTIF(result = 'PASS') AS DOUBLE) / NULLIF(COUNT(*), 0) * 100, 2
        )                            AS pass_rate_pct,
        AVG(result_value)            AS avg_result_value
    FROM {{ ref('silver_iqms_quality_tests') }}
    GROUP BY 1, 2
),

final AS (
    SELECT
        p.machine_id,
        p.plant,
        p.shift,
        p.production_date,
        p.year,
        p.month,
        p.day,
        -- Volume metrics
        p.total_orders,
        p.total_planned_qty,
        p.total_actual_qty,
        p.total_rejected_qty,
        p.completed_orders,
        p.on_hold_orders,
        -- Efficiency
        COALESCE(p.avg_yield_pct, 0)  AS avg_yield_pct,
        COALESCE(p.avg_scrap_pct, 0)  AS avg_scrap_pct,
        p.total_production_minutes,
        -- OEE components (simplified)
        ROUND(
            CAST(p.completed_orders AS DOUBLE) / NULLIF(p.total_orders, 0), 4
        )                             AS availability,
        ROUND(
            COALESCE(p.avg_yield_pct, 0) / 100, 4
        )                             AS performance,
        COALESCE(q.pass_rate_pct, 0) / 100 AS quality_rate,
        -- OEE Score
        ROUND(
            (CAST(p.completed_orders AS DOUBLE) / NULLIF(p.total_orders, 0))
            * (COALESCE(p.avg_yield_pct, 0) / 100)
            * (COALESCE(q.pass_rate_pct, 0) / 100),
            4
        )                             AS oee_score,
        -- Quality
        COALESCE(q.total_tests, 0)    AS quality_tests_run,
        COALESCE(q.passed_tests, 0)   AS quality_tests_passed,
        COALESCE(q.pass_rate_pct, 0)  AS quality_pass_rate_pct,
        -- Audit
        CURRENT_TIMESTAMP             AS _gold_loaded_at,
        'tpl_lakehouse.dbt'           AS _data_source
    FROM production p
    LEFT JOIN quality q
        ON p.machine_id = q.machine_id
        AND p.production_date = q.quality_date
)

SELECT * FROM final
ORDER BY production_date DESC, machine_id
