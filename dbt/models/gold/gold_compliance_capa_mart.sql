{{
  config(
    materialized='table',
    tags=['gold', 'compliance', 'capa', 'regulatory']
  )
}}

/*
  Gold Layer: Compliance & CAPA Performance Mart
  21 CFR Part 11 / EU Annex 11 / ALCOA+ aligned
  Powers: QA dashboards, audit-readiness reports, deviation trend analysis
*/

WITH capas AS (
    SELECT
        capa_id,
        capa_type,
        source                          AS capa_source,
        product_code,
        department,
        owner,
        status,
        CAST(opened_date AS DATE)        AS opened_date,
        CAST(target_close_date AS DATE)  AS target_close_date,
        CAST(actual_close_date AS DATE)  AS actual_close_date,
        effectiveness_verified,
        -- SLA compliance
        CASE
            WHEN actual_close_date IS NOT NULL
                THEN DATE_DIFF('day', CAST(opened_date AS DATE), CAST(actual_close_date AS DATE))
            ELSE DATE_DIFF('day', CAST(opened_date AS DATE), CURRENT_DATE)
        END                              AS days_open,
        CASE
            WHEN actual_close_date IS NULL AND CURRENT_DATE > CAST(target_close_date AS DATE)
                THEN TRUE ELSE FALSE
        END                              AS is_overdue,
        _ingested_at
    FROM {{ ref('silver_trackwise_capas') }}
),

deviations AS (
    SELECT
        product_code,
        severity,
        DATE_TRUNC('month', CAST(detected_at AS DATE)) AS month_year,
        COUNT(*)                                        AS deviation_count
    FROM {{ ref('silver_iqms_deviations') }}
    GROUP BY 1, 2, 3
),

capa_summary AS (
    SELECT
        product_code,
        department,
        DATE_TRUNC('month', opened_date)                    AS report_month,
        COUNT(*)                                            AS total_capas,
        COUNTIF(status = 'CLOSED')                         AS closed_capas,
        COUNTIF(status IN ('OPEN', 'IN_PROGRESS'))         AS open_capas,
        COUNTIF(is_overdue = TRUE)                         AS overdue_capas,
        COUNTIF(effectiveness_verified = TRUE)              AS effectiveness_verified_count,
        AVG(days_open)                                     AS avg_days_open,
        MAX(days_open)                                     AS max_days_open,
        ROUND(
            CAST(COUNTIF(status = 'CLOSED') AS DOUBLE) / NULLIF(COUNT(*), 0) * 100, 2
        )                                                  AS closure_rate_pct
    FROM capas
    GROUP BY 1, 2, 3
)

SELECT
    cs.*,
    COALESCE(d.deviation_count, 0)                         AS linked_deviations_critical,
    -- Compliance KPI
    CASE
        WHEN cs.closure_rate_pct >= 90 THEN 'GREEN'
        WHEN cs.closure_rate_pct >= 70 THEN 'AMBER'
        ELSE 'RED'
    END                                                    AS compliance_rag_status,
    CURRENT_TIMESTAMP                                      AS _gold_loaded_at
FROM capa_summary cs
LEFT JOIN deviations d
    ON cs.product_code = d.product_code
    AND d.severity = 'CRITICAL'
    AND d.month_year = cs.report_month
ORDER BY cs.report_month DESC, cs.department
