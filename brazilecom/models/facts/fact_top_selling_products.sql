-- Fact Table: fact_top_selling_products
SELECT 
    doi.product_id,
    dp.product_category,
    COUNT(doi.order_id) AS total_sales,
    SUM(doi.price) AS total_revenue

FROM {{ ref('dim_order_items') }} doi

JOIN {{ ref('dim_products') }} dp ON doi.product_id = dp.product_id

GROUP BY doi.product_id, dp.product_category
ORDER BY total_sales DESC