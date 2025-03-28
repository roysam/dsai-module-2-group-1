
WITH ranked_products AS (
    -- Get the top-selling product per seller
    SELECT 
        doi.seller_id,
        doi.product_id,
        dp.product_category,
        SUM(doi.price) AS total_revenue,
        RANK() OVER (PARTITION BY doi.seller_id ORDER BY SUM(doi.price) DESC) AS product_rank
    
    FROM {{ ref('fact_order_items') }} doi

    JOIN  {{ ref('dim_products') }} dp ON doi.product_id = dp.product_id

    GROUP BY doi.seller_id, doi.product_id, dp.product_category
), ranked_locations AS (
    -- Find the location where this top-selling product was sold the most
    SELECT 
        doi.seller_id,
        doi.product_id,
        dg.geolocation_state,
        dg.geolocation_city,
        SUM(doi.price) AS location_revenue,
        RANK() OVER (PARTITION BY doi.seller_id, doi.product_id ORDER BY SUM(doi.price) DESC) AS location_rank
    
    FROM  {{ ref('fact_order_items') }} doi

    JOIN  {{ ref('fact_orders') }} do ON doi.order_id = do.order_id
    
    JOIN  {{ ref('dim_customer') }} dc ON do.customer_id = dc.customer_id

    JOIN  {{ ref('dim_geolocation') }} dg ON dc.customer_zip_code_prefix = dg.geolocation_zip_code_prefix
    
    GROUP BY doi.seller_id, doi.product_id, dg.geolocation_state, dg.geolocation_city
)
SELECT 
    rp.seller_id,
    rp.product_id,
    rp.product_category,
    rp.total_revenue AS product_total_revenue,
    rl.geolocation_state,
    rl.geolocation_city,
    rl.location_revenue
FROM ranked_products rp
JOIN ranked_locations rl 
    ON rp.seller_id = rl.seller_id 
    AND rp.product_id = rl.product_id
WHERE rp.product_rank = 1 
AND rl.location_rank = 1
