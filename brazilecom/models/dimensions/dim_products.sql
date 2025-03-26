-- Dimension Table: dim_products
SELECT 
    p.product_id,
    COALESCE(pt.product_category_name_english, 'Unknown') AS product_category,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM {{ source('brazilecom_raw_data', 'products') }} p

LEFT JOIN {{ source('brazilecom_raw_data', 'product_translation') }} pt ON p.product_category_name = pt.product_category_name