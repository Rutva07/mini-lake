-- Daily funnel: scans only requested date partitions when used with a bounded date filter.
SELECT
    event_date,
    COUNT_IF(event_type = 'page_view') AS page_views,
    COUNT_IF(event_type = 'add_to_cart') AS adds_to_cart,
    COUNT_IF(event_type = 'purchase') AS purchases,
    ROUND(100 * purchases / NULLIF(page_views, 0), 2) AS conversion_rate_pct
FROM MINILAKE.ANALYTICS.EVENTS
WHERE event_date BETWEEN DATEADD(day, -30, CURRENT_DATE()) AND CURRENT_DATE()
GROUP BY event_date
ORDER BY event_date;

-- Revenue by market.
SELECT
    country,
    COUNT(*) AS orders,
    ROUND(SUM(amount), 2) AS revenue,
    ROUND(AVG(amount), 2) AS average_order_value
FROM MINILAKE.ANALYTICS.EVENTS
WHERE event_type = 'purchase'
  AND event_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY country
ORDER BY revenue DESC;

