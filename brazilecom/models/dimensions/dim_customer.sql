-- Dimension Table: dim_customer

SELECT DISTINCT 
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
FROM {{ source('brazilecom_raw_data', 'customer') }} c
