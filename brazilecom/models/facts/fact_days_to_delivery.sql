-- Fact Table: fact_days_to_delivery

SELECT 
    do.order_id,
    do.customer_id,
    do.order_purchase_timestamp,
    do.order_delivered_customer_date,
    do.order_status,
    DATE_DIFF(do.order_delivered_customer_date, do.order_purchase_timestamp, DAY) AS days_to_delivery

FROM {{ ref('fact_orders') }} do

WHERE LOWER(order_status) = 'delivered' 

AND order_delivered_customer_date IS NOT NULL