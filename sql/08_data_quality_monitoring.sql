-- 08_data_quality_monitoring.sql
-- Quality SLA monitoring: Null field audit, negative value detection, and quarantine volume

SELECT 
    'Clean Table Null Check' AS check_name,
    COUNT(*) AS total_rows_inspected,
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_txn_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_cust_ids,
    SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) AS non_positive_amounts,
    SUM(CASE WHEN discount_amount > gross_amount THEN 1 ELSE 0 END) AS invalid_discounts
FROM retail_transactions

UNION ALL

SELECT 
    'Quarantine Reason Distribution' AS check_name,
    COUNT(*) AS total_quarantined_records,
    SUM(CASE WHEN rejection_reason LIKE '%ERR_DUPLICATE_TXN_ID%' THEN 1 ELSE 0 END) AS duplicate_txn_failures,
    SUM(CASE WHEN rejection_reason LIKE '%ERR_NON_POSITIVE_PRICE%' OR rejection_reason LIKE '%ERR_NON_POSITIVE_QUANTITY%' THEN 1 ELSE 0 END) AS numeric_range_failures,
    SUM(CASE WHEN rejection_reason LIKE '%ERR_DISCOUNT_EXCEEDS_GROSS%' THEN 1 ELSE 0 END) AS business_rule_failures,
    SUM(CASE WHEN rejection_reason LIKE '%ERR_NULL%' THEN 1 ELSE 0 END) AS missing_key_failures
FROM rejected_transactions;

