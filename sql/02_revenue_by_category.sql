-- 02_revenue_by_category.sql
-- Revenue, sales volume, margin, and discount rates broken down by product category

SELECT 
    product_category,
    COUNT(transaction_id) AS transaction_count,
    SUM(quantity) AS units_sold,
    ROUND(SUM(gross_amount), 2) AS category_gross_sales,
    ROUND(SUM(discount_amount), 2) AS total_discounts,
    ROUND(SUM(net_amount), 2) AS category_net_sales,
    ROUND(SUM(total_amount), 2) AS category_total_billed,
    ROUND(AVG(total_amount), 2) AS avg_ticket_size,
    ROUND((SUM(discount_amount) / NULLIF(SUM(gross_amount), 0)) * 100, 2) AS discount_penetration_pct
FROM retail_transactions
GROUP BY product_category
ORDER BY category_total_billed DESC;

