{{
  config(
    materialized='table',
    tags=['gold', 'training', 'compliance', 'gmp']
  )
}}

/*
  Gold Layer: Training Compliance Mart
  Grain: department + month
  Powers: GMP compliance dashboards, training gap analysis, audit readiness
  21 CFR Part 11 — demonstrable training compliance
*/

WITH training_data AS (
    SELECT
        department,
        DATE_TRUNC('month', COALESCE(completion_date, scheduled_date)) AS report_month,
        record_id,
        employee_id,
        training_category,
        status,
        score,
        training_overdue_flag,
        certification_expired,
        days_since_completion
    FROM {{ ref('silver_tms_training') }}
),

dept_summary AS (
    SELECT
        department,
        report_month,
        -- Headcount metrics
        COUNT(DISTINCT employee_id)                              AS total_employees,
        COUNT(*)                                                  AS total_training_records,
        -- Completion metrics
        COUNTIF(status = 'COMPLETED')                           AS completed_trainings,
        COUNTIF(status IN ('SCHEDULED', 'IN_PROGRESS'))         AS pending_trainings,
        COUNTIF(training_overdue_flag = TRUE)                    AS overdue_trainings,
        -- Distinct employees who completed at least one training
        COUNT(DISTINCT CASE WHEN status = 'COMPLETED'
                            THEN employee_id END)                AS total_employees_trained,
        -- Completion rate
        ROUND(
            CAST(COUNTIF(status = 'COMPLETED') AS DOUBLE) /
            NULLIF(COUNT(*), 0) * 100, 2
        )                                                        AS completion_rate_pct,
        -- GMP-specific metrics
        COUNTIF(training_category = 'GMP' AND status = 'COMPLETED')  AS gmp_completed,
        COUNTIF(training_category = 'GMP')                           AS gmp_total,
        -- Score metrics
        AVG(CASE WHEN status = 'COMPLETED' THEN score END)      AS avg_score,
        MIN(CASE WHEN status = 'COMPLETED' THEN score END)      AS min_score,
        -- Certification health
        COUNTIF(certification_expired = TRUE)                   AS expired_certifications
    FROM training_data
    GROUP BY 1, 2
),

final AS (
    SELECT
        department,
        report_month,
        total_employees,
        total_employees_trained,
        total_training_records,
        completed_trainings,
        pending_trainings,
        overdue_trainings,
        expired_certifications,
        completion_rate_pct,
        -- GMP compliance score (0-100)
        CASE WHEN gmp_total > 0
             THEN ROUND(CAST(gmp_completed AS DOUBLE) / gmp_total * 100, 2)
             ELSE 0.0
        END                                                      AS gmp_compliance_score,
        COALESCE(avg_score, 0)                                   AS avg_training_score,
        COALESCE(min_score, 0)                                   AS min_training_score,
        -- Training RAG status
        CASE
            WHEN completion_rate_pct >= 95.0 AND overdue_trainings = 0
                THEN 'GREEN'
            WHEN completion_rate_pct >= 80.0 AND overdue_trainings <= 3
                THEN 'AMBER'
            ELSE 'RED'
        END                                                      AS training_rag_status,
        -- Audit
        CURRENT_TIMESTAMP                                        AS _gold_loaded_at,
        'tpl_lakehouse.dbt'                                      AS _data_source
    FROM dept_summary
)

SELECT * FROM final
ORDER BY report_month DESC, department
