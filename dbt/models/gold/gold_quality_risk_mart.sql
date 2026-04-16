{{
  config(
    materialized='table',
    tags=['gold', 'quality', 'risk', 'batch_release']
  )
}}

/*
  Gold Layer: Quality Risk Assessment Mart
  Grain: product_code + batch_number
  Joins: silver_mes + silver_iqms_quality_tests + silver_iqms_deviations
  Powers: batch release decisions, risk scoring, quality trend analysis
  21 CFR Part 11 / ALCOA+ aligned
*/

WITH production AS (
    SELECT
        product_code,
        batch_number,
        COUNT(*)                     AS total_production_orders,
        SUM(actual_qty)              AS total_actual_qty,
        SUM(rejected_qty)            AS total_rejected_qty,
        AVG(yield_pct)               AS avg_yield_pct,
        AVG(scrap_pct)               AS avg_scrap_pct
    FROM {{ ref('silver_mes_production_orders') }}
    GROUP BY 1, 2
),

quality_tests AS (
    SELECT
        product_code,
        batch_number,
        COUNT(*)                                                 AS total_tests,
        COUNTIF(pass_fail_flag = TRUE)                          AS passed_tests,
        COUNTIF(pass_fail_flag = FALSE)                         AS failed_tests,
        ROUND(
            CAST(COUNTIF(pass_fail_flag = TRUE) AS DOUBLE) /
            NULLIF(COUNT(*), 0) * 100, 2
        )                                                        AS pass_rate,
        AVG(result_value)                                        AS avg_result_value,
        AVG(deviation_from_mean)                                 AS avg_deviation_from_mean,
        COUNTIF(spec_status = 'ABOVE_USL')                      AS above_usl_count,
        COUNTIF(spec_status = 'BELOW_LSL')                      AS below_lsl_count
    FROM {{ ref('silver_iqms_quality_tests') }}
    GROUP BY 1, 2
),

deviations AS (
    SELECT
        product_code,
        batch_number,
        COUNT(*)                                                 AS total_deviations,
        COUNTIF(is_open = TRUE)                                 AS open_deviations,
        COUNTIF(severity = 'CRITICAL')                          AS critical_deviation_count,
        COUNTIF(severity = 'MAJOR')                             AS major_deviation_count,
        COUNTIF(severity = 'MINOR')                             AS minor_deviation_count,
        AVG(severity_score)                                      AS avg_severity_score,
        MAX(severity_score)                                      AS max_severity_score
    FROM {{ ref('silver_iqms_deviations') }}
    GROUP BY 1, 2
),

final AS (
    SELECT
        COALESCE(p.product_code, qt.product_code, d.product_code) AS product_code,
        COALESCE(p.batch_number, qt.batch_number, d.batch_number) AS batch_number,
        -- Production metrics
        COALESCE(p.total_production_orders, 0)   AS total_production_orders,
        COALESCE(p.total_actual_qty, 0)          AS total_actual_qty,
        COALESCE(p.total_rejected_qty, 0)        AS total_rejected_qty,
        COALESCE(p.avg_yield_pct, 0)             AS avg_yield_pct,
        -- Quality test metrics
        COALESCE(qt.total_tests, 0)              AS total_tests,
        COALESCE(qt.passed_tests, 0)             AS passed_tests,
        COALESCE(qt.failed_tests, 0)             AS failed_tests,
        COALESCE(qt.pass_rate, 0)                AS pass_rate,
        -- Deviation metrics
        COALESCE(d.total_deviations, 0)          AS total_deviations,
        COALESCE(d.open_deviations, 0)           AS open_deviations,
        COALESCE(d.critical_deviation_count, 0)  AS critical_deviation_count,
        COALESCE(d.major_deviation_count, 0)     AS major_deviation_count,
        -- Batch release decision
        CASE
            WHEN d.critical_deviation_count > 0 OR d.open_deviations > 0
                THEN 'PENDING'
            WHEN qt.pass_rate IS NULL
                THEN 'PENDING'
            WHEN qt.pass_rate >= 95.0 AND COALESCE(d.critical_deviation_count, 0) = 0
                THEN 'PASS'
            WHEN qt.pass_rate < 80.0 OR d.critical_deviation_count > 0
                THEN 'FAIL'
            ELSE 'PENDING'
        END                                      AS batch_release_status,
        -- Overall risk score (0-100, higher = more risk)
        ROUND(
            LEAST(100.0,
                (100.0 - COALESCE(qt.pass_rate, 50.0))
                + COALESCE(d.critical_deviation_count, 0) * 20.0
                + COALESCE(d.major_deviation_count, 0) * 10.0
                + COALESCE(d.minor_deviation_count, 0) * 3.0
                + CASE WHEN COALESCE(p.avg_yield_pct, 100) < 85 THEN 15.0 ELSE 0.0 END
            ), 2
        )                                        AS overall_risk_score,
        -- Risk RAG status
        CASE
            WHEN COALESCE(d.critical_deviation_count, 0) > 0 THEN 'RED'
            WHEN COALESCE(d.open_deviations, 0) > 2 OR COALESCE(qt.pass_rate, 100) < 90 THEN 'AMBER'
            ELSE 'GREEN'
        END                                      AS risk_rag_status,
        -- Audit
        CURRENT_TIMESTAMP                        AS _gold_loaded_at,
        'tpl_lakehouse.dbt'                      AS _data_source
    FROM production p
    FULL OUTER JOIN quality_tests qt
        ON p.product_code = qt.product_code AND p.batch_number = qt.batch_number
    FULL OUTER JOIN deviations d
        ON COALESCE(p.product_code, qt.product_code) = d.product_code
        AND COALESCE(p.batch_number, qt.batch_number) = d.batch_number
)

SELECT * FROM final
ORDER BY overall_risk_score DESC, product_code
