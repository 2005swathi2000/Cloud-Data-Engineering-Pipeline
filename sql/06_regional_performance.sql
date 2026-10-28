-- 06_regional_performance.sql
-- Regional revenue distribution, average basket size, and channel performance

SELECT 
    store_region,
    COUNT(DISTINCT store_id) AS active_stores_or_channels,
    COUNT(transaction_id) AS total_regional_transactions,
    SUM(quantity) AS units_sold,
    ROUND(SUM(total_amount), 2) AS regional_revenue,
    ROUND(AVG(total_amount), 2) AS regional_aov,
    ROUND((SUM(total_amount) / (SELECT SUM(total_amount) FROM retail_transactions)) * 100, 2) AS revenue_share_pct
FROM retail_transactions
GROUP BY store_region
ORDER BY regional_revenue DESC;

