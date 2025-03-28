-- Dimension Table: dim_sellers
SELECT DISTINCT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state
FROM {{ source('brazilecom_raw_data', 'sellers') }} s
