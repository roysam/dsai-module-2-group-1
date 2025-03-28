
-- Use the `ref` function to select from other models

-- Orders table 
WITH 
    orders as (
        select * from {{source('brazilecom', "orders")}}
    ),
    orderitems as (
        select * from {{source('brazilecom', "order_items")}}
    ),
    combined as (
    -- only look at the confirmed order. Exclude (Canceled and Created)
        select orders.order_id, orders.customer_id, orders.order_status, 
        EXTRACT(YEAR FROM date(orders.order_purchase_timestamp)) as order_year,
        orders.order_purchase_timestamp, orders.order_approved_at, 
        orders.order_delivered_carrier_date, orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date, 
        orderitems.order_item_id, 
        orderitems.product_id, 
        orderitems.seller_id, 
        orderitems.price, 
        orderitems.freight_value
        from
        orders
        left join 
        orderitems
        on orders.order_id = orderitems.order_id 
        where orders.order_status not in ('canceled','created','unavailable')
    ),
    orderitemsseller as (
        select 
            a.order_id, 
            a.customer_id, 
            a.order_status, 
            a.order_year, 
            a.order_purchase_timestamp, 
            a.order_approved_at, 
            a.order_delivered_carrier_date, 
            a.order_delivered_customer_date, 
            a.order_estimated_delivery_date, 
            a.order_item_id, 
            a.product_id, 
            a.seller_id, 
            a.price, 
            a.freight_value, 
            b.seller_city, 
            b.seller_state
        from combined a
        left join 
        {{ref("dim_sellers")}} b
        on a.seller_id = b.seller_id
    ), 
    orderitemssellercust as (
        select 
            a.order_id, 
            a.customer_id, 
            a.order_status, 
            a.order_year, 
            a.order_purchase_timestamp, 
            a.order_approved_at, 
            a.order_delivered_carrier_date, 
            a.order_delivered_customer_date, 
            a.order_estimated_delivery_date, 
            a.order_item_id, 
            a.product_id, 
            a.seller_id, 
            a.price, 
            a.freight_value, 
            a.seller_city, 
            a.seller_state,
            b.customer_city,
            b.customer_state,
            b.customer_unique_id
        from orderitemsseller a
        left join 
        {{ref("dim_customers")}} b
        on a.customer_id = b.customer_id
    )

select 
    a.order_id,
    a.customer_id, 
    a.order_status, 
    a.order_year, 
    a.order_purchase_timestamp, 
    a.order_approved_at, 
    a.order_delivered_carrier_date, 
    a.order_delivered_customer_date, 
    a.order_estimated_delivery_date, 
    a.order_item_id, 
    a.product_id, 
    a.seller_id, 
    a.price, 
    a.freight_value, 
    a.seller_city, 
    a.seller_state, 
    a.customer_city,
    a.customer_state,
    a.customer_unique_id,
    prodts.product_category_name_english, 
    prodts.product_name_lenght, 
    prodts.product_description_lenght,
    prodts.product_photos_qty, 
    prodts.product_weight_g,
    prodts.product_length_cm,
    prodts.product_height_cm,
    prodts.product_width_cm
from orderitemssellercust a
left join 
{{ref("dim_products")}} prodts
on a.product_id = prodts.product_id
    

