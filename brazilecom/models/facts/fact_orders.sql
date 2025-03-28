-- Dimension Table: dim_orders

SELECT 
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM {{ source('brazilecom_raw_data', 'orders') }} o

WHERE order_status IN ('delivered', 'invoiced', 'shipped', 'processing')