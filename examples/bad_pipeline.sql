-- Intentionally flawed: use to demo `de review examples/bad_pipeline.sql`
INSERT INTO analytics.daily_revenue
SELECT o.order_date, SUM(o.amount) AS revenue
FROM orders o
JOIN order_items i ON i.order_id = o.order_id   -- fan-out
WHERE YEAR(o.order_date) = 2024                 -- non-SARGable
  AND o.customer_id NOT IN (SELECT customer_id FROM blocked_customers)
GROUP BY o.order_date;
