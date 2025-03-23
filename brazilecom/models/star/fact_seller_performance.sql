-- Fact Table: fact_seller_performance
SELECT 
    oi.seller_id,
    DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS month,
    DATE_TRUNC(DATE(o.order_purchase_timestamp), QUARTER) AS quarter,
    SUM(oi.price) AS total_turnover,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT oi.product_id) AS total_products_sold
FROM 
    `dsai-module-2-project.olist.orders` o
JOIN 
    `dsai-module-2-project.olist.order_items` oi 
ON 
    o.order_id = oi.order_id
GROUP BY 
    oi.seller_id, month, quarter
ORDER BY total_turnover DESC