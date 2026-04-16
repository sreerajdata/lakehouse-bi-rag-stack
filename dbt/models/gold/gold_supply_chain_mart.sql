{{
  config(
    materialized='table',
    tags=['gold', 'supply_chain', 'vendor', 'procurement']
  )
}}

/*
  Gold Layer: Supply Chain & Vendor Performance Mart
  Grain: vendor_code + material_code + month
  Joins: silver_sap_inventory + silver_trackwise_capas (vendor-linked)
  Powers: vendor scorecards, procurement analytics, supply risk assessment
*/

WITH inventory AS (
    SELECT
        vendor_code,
        material_code,
        plant,
        posting_month,
        COUNT(*)                                                 AS total_movements,
        SUM(quantity)                                             AS total_quantity,
        SUM(valuation_amount_inr)                                AS total_value_inr,
        SUM(CASE WHEN movement_category = 'receipt'
                 THEN quantity ELSE 0 END)                       AS receipt_qty,
        SUM(CASE WHEN movement_category = 'receipt'
                 THEN valuation_amount_inr ELSE 0 END)           AS receipt_value_inr,
        SUM(CASE WHEN movement_category = 'scrap'
                 THEN quantity ELSE 0 END)                       AS scrap_qty,
        SUM(CASE WHEN movement_category = 'scrap'
                 THEN valuation_amount_inr ELSE 0 END)           AS scrap_value_inr,
        COUNT(DISTINCT batch_number)                             AS unique_batches
    FROM {{ ref('silver_sap_inventory') }}
    WHERE vendor_code IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

vendor_capas AS (
    SELECT
        -- Link CAPAs to vendors via product_code
        product_code,
        DATE_TRUNC('month', opened_date)                         AS capa_month,
        COUNT(*)                                                  AS vendor_related_capas,
        COUNTIF(status IN ('OPEN', 'IN_PROGRESS'))               AS open_capas,
        COUNTIF(sla_breach_flag = TRUE)                          AS sla_breaches,
        COUNTIF(is_overdue = TRUE)                               AS overdue_capas,
        AVG(days_open)                                            AS avg_capa_days_open
    FROM {{ ref('silver_trackwise_capas') }}
    WHERE source IN ('Deviation', 'Customer Complaint', 'OOS')
    GROUP BY 1, 2
),

final AS (
    SELECT
        i.vendor_code,
        i.material_code,
        i.plant,
        i.posting_month,
        -- Volume metrics
        i.total_movements,
        i.total_quantity,
        i.receipt_qty,
        i.unique_batches,
        -- Financial metrics
        i.total_value_inr                                        AS total_po_value_inr,
        i.receipt_value_inr,
        i.scrap_value_inr,
        -- Quality metrics
        CASE WHEN i.receipt_qty > 0
             THEN ROUND(CAST(i.scrap_qty AS DOUBLE) / i.receipt_qty * 100, 2)
             ELSE 0.0
        END                                                      AS quality_rejection_rate_pct,
        -- Lead time approximation (avg days between PO months)
        ROUND(30.0 / NULLIF(i.total_movements, 0), 1)           AS avg_lead_time_days,
        -- On-time delivery approximation (% of non-scrap receipts)
        CASE WHEN i.receipt_qty > 0
             THEN ROUND(
                 CAST(i.receipt_qty - i.scrap_qty AS DOUBLE) / i.receipt_qty * 100, 2
             )
             ELSE 0.0
        END                                                      AS on_time_delivery_pct,
        -- CAPA metrics (linked via product)
        COALESCE(vc.vendor_related_capas, 0)                     AS vendor_related_capas,
        COALESCE(vc.open_capas, 0)                               AS vendor_open_capas,
        COALESCE(vc.sla_breaches, 0)                             AS vendor_sla_breaches,
        -- Composite vendor score (0-100, higher = better)
        ROUND(
            GREATEST(0.0, LEAST(100.0,
                100.0
                - COALESCE(
                    CAST(i.scrap_qty AS DOUBLE) / NULLIF(i.receipt_qty, 0) * 30, 0)
                - COALESCE(vc.vendor_related_capas, 0) * 5.0
                - COALESCE(vc.sla_breaches, 0) * 10.0
            )), 2
        )                                                        AS vendor_score,
        -- Vendor tier
        CASE
            WHEN ROUND(
                GREATEST(0.0, LEAST(100.0,
                    100.0
                    - COALESCE(
                        CAST(i.scrap_qty AS DOUBLE) / NULLIF(i.receipt_qty, 0) * 30, 0)
                    - COALESCE(vc.vendor_related_capas, 0) * 5.0
                    - COALESCE(vc.sla_breaches, 0) * 10.0
                )), 2) >= 85 THEN 'PREFERRED'
            WHEN ROUND(
                GREATEST(0.0, LEAST(100.0,
                    100.0
                    - COALESCE(
                        CAST(i.scrap_qty AS DOUBLE) / NULLIF(i.receipt_qty, 0) * 30, 0)
                    - COALESCE(vc.vendor_related_capas, 0) * 5.0
                    - COALESCE(vc.sla_breaches, 0) * 10.0
                )), 2) >= 60 THEN 'APPROVED'
            ELSE 'UNDER_REVIEW'
        END                                                      AS vendor_tier,
        -- Audit
        CURRENT_TIMESTAMP                                        AS _gold_loaded_at,
        'tpl_lakehouse.dbt'                                      AS _data_source
    FROM inventory i
    LEFT JOIN vendor_capas vc
        ON i.material_code = vc.product_code
        AND i.posting_month = vc.capa_month
)

SELECT * FROM final
ORDER BY posting_month DESC, vendor_score ASC
