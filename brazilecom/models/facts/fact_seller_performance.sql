-- Fact Table: fact_seller_performance
SELECT 
    doi.seller_id,
    DATE_TRUNC(DATE(do.order_purchase_timestamp), MONTH) AS month,
    DATE_TRUNC(DATE(do.order_purchase_timestamp), QUARTER) AS quarter,
    SUM(doi.price) AS total_turnover,
    COUNT(DISTINCT do.order_id) AS total_orders,
    COUNT(DISTINCT doi.product_id) AS total_products_sold
FROM 
    {{ ref('dim_orders') }} do
JOIN 
    {{ ref('dim_order_items') }} doi
ON 
    do.order_id = doi.order_id
GROUP BY 
    doi.seller_id, month, quarter
    
ORDER BY total_turnover DESC