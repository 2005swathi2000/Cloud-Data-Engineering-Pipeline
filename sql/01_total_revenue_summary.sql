-- 01_total_revenue_summary.sql
-- High-level financial KPIs: Total gross/net revenue, tax collected, order volume, and Average Order Value (AOV)

SELECT 
    COUNT(DISTINCT transaction_id) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(gross_amount), 2) AS total_gross_revenue,
    ROUND(SUM(discount_amount), 2) AS total_discounts_granted,
    ROUND(SUM(net_amount), 2) AS total_net_revenue,
    ROUND(SUM(tax_amount), 2) AS total_sales_tax_collected,
    ROUND(SUM(total_amount), 2) AS total_billed_revenue,
    ROUND(AVG(total_amount), 2) AS average_order_value_aov,
    SUM(quantity) AS total_items_sold
FROM retail_transactions;

