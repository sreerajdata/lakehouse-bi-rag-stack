SELECT 'mes_events' AS table_name, COUNT(*) AS row_count FROM iceberg.bronze.mes_events
UNION ALL
SELECT 'iqms_orders', COUNT(*) FROM iceberg.bronze.iqms_orders
UNION ALL
SELECT 'trackwise_deviations', COUNT(*) FROM iceberg.bronze.trackwise_deviations
UNION ALL
SELECT 'sap_ecc_orders', COUNT(*) FROM iceberg.bronze.sap_ecc_orders
UNION ALL
SELECT 'sop_documents', COUNT(*) FROM iceberg.bronze.sop_documents
