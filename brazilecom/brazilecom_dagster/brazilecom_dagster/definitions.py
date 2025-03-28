from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource

from .assets import ( 
    brazilecom_dbt_assets, 
    #missing_dimension_check, 
    download_data, 
    monthly_sales_performance, 
    customer, 
    sellers, 
    orders, 
    geolocation, 
    products, 
    product_translation, 
    order_items, 
    payments, 
    order_reviews,
)

from .project import brazilecom_project
from .schedules import schedules

defs = Definitions(
    assets=[
        download_data,
        sellers,
        customer,
        orders,
        geolocation, 
        products, 
        product_translation, 
        order_items,    
        payments, 
        order_reviews,
        monthly_sales_performance,
        brazilecom_dbt_assets,
       ],
    #asset_checks=[missing_dimension_check],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=brazilecom_project),
        "bigquery": BigQueryResource(project="premium-node-451703-i2", 
                                     gcp_credentials="D:/Projects/ntu/course/dsai-module-2-group-1/brazilecom/dsai-module-2-project-839a141670bd.json"),
    },
)
