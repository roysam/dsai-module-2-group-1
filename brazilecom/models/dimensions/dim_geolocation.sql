-- Dimension Table: dim_geolocation
SELECT 
    g.geolocation_zip_code_prefix,
    g.geolocation_lat,
    g.geolocation_lng,
    g.geolocation_city,
    g.geolocation_state
FROM {{ source('brazilecom_raw_data', 'geolocation') }} g