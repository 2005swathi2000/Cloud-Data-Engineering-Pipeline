-- 05_customer_summaries.sql
-- Customer lifetime value (LTV), purchase frequency, and preferred payment method
-- Demonstrates PII masking with customer_masked_email

SELECT 
    customer_id,
    MAX(customer_masked_email) AS masked_email,
    COUNT(transaction_id) AS total_orders,
    SUM(quantity) AS total_items_bought,
    ROUND(SUM(total_amount), 2) AS customer_lifetime_spend,
    ROUND(AVG(total_amount), 2) AS customer_avg_order_value,
    MIN(transaction_date) AS first_purchase_date,
    MAX(transaction_date) AS most_recent_purchase_date
FROM retail_transactions
GROUP BY customer_id
ORDER BY customer_lifetime_spend DESC
LIMIT 15;

