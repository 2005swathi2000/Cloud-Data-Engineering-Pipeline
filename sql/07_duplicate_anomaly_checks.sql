-- 07_duplicate_anomaly_checks.sql
-- Governance audit query: Asserts zero duplicate primary keys in the clean analytical warehouse
-- and identifies quarantined anomaly patterns

-- Query 1: Uniqueness verification (Should return 0 rows in production clean table)
SELECT 
    transaction_id,
    COUNT(*) AS occurrence_count
FROM retail_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

