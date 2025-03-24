With 

prodts as (
    select product_id, product_category_name, 
        product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,
        product_width_cm 
        from {{source('brazilecom','products')}}
),

prodtcat as (
    select product_category_name, product_category_name_english from {{source('brazilecom','product_category_name_translation')}}
)

select product_id, product_category_name_english,
       product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,
       product_width_cm 
       from prodts left join prodtcat on prodts.product_category_name = prodtcat.product_category_name 

