-- 03_revenue_trends.sql
-- Time-series aggregation: Daily and weekly revenue velocity and cumulative sales trend

SELECT 
    transaction_date,
    day_of_week,
    COUNT(transaction_id) AS daily_transactions,
    SUM(quantity) AS daily_units_sold,
    ROUND(SUM(total_amount), 2) AS daily_revenue,
    ROUND(AVG(total_amount), 2) AS daily_avg_order_value,
    ROUND(SUM(SUM(total_amount)) OVER (ORDER BY transaction_date), 2) AS running_cumulative_revenue
FROM retail_transactions
GROUP BY transaction_date, day_of_week
ORDER BY transaction_date ASC;

