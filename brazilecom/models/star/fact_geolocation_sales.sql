-- Fact Table: fact_geolocation_sales
SELECT 
    g.geolocation_state,
    g.geolocation_city,
    oi.product_id,
    COUNT(o.order_id) AS total_orders,
    SUM(oi.price) AS total_revenue
FROM `dsai-module-2-project.olist.orders` o

JOIN `dsai-module-2-project.olist.order_items` oi ON o.order_id = oi.order_id

JOIN `dsai-module-2-project.olist.customer` c ON o.customer_id = c.customer_id

JOIN `dsai-module-2-project.olist.geolocation` g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix

GROUP BY g.geolocation_state, g.geolocation_city, oi.product_id

ORDER BY total_revenue DESC
