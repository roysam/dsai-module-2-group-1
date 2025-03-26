-- Fact Table: fact_geolocation_sales
SELECT 
    dg.geolocation_state,
    dg.geolocation_city,
    doi.product_id,
    dp.product_category,
    COUNT(do.order_id) AS total_orders,
    SUM(doi.price) AS total_revenue

FROM {{ ref('dim_orders') }} do


JOIN {{ ref('dim_order_items') }} doi ON do.order_id = doi.order_id

JOIN {{ ref('dim_customer') }} dc ON do.customer_id = dc.customer_id

JOIN {{ ref('dim_geolocation') }} dg ON dc.customer_zip_code_prefix = dg.geolocation_zip_code_prefix

JOIN {{ ref('dim_products') }} dp ON doi.product_id = dp.product_id

GROUP BY dg.geolocation_state, dg.geolocation_city, doi.product_id,dp.product_category

ORDER BY total_revenue DESC