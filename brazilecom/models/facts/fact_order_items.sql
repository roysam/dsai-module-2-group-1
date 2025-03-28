-- Dimension Table: dim_orders_items

SELECT 
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value
FROM {{ source('brazilecom_raw_data', 'order_items') }} oi
