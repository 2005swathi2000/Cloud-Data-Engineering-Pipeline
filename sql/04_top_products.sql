-- 04_top_products.sql
-- Top 10 best-selling products by total billed revenue and units moved

SELECT 
    product_id,
    product_name,
    product_category,
    COUNT(transaction_id) AS orders_count,
    SUM(quantity) AS total_units_sold,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    ROUND(SUM(gross_amount), 2) AS total_gross_sales,
    ROUND(SUM(total_amount), 2) AS total_revenue_generated
FROM retail_transactions
GROUP BY product_id, product_name, product_category
ORDER BY total_revenue_generated DESC
LIMIT 10;

