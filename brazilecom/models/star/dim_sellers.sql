
select seller_id, seller_city,seller_state from {{ source('brazilecom', 'sellers') }} 