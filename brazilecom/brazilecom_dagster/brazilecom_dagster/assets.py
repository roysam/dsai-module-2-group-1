from dagster import AssetExecutionContext,MetadataValue, MaterializeResult, asset, asset_check, AssetCheckResult, AutomationCondition
from dagster_dbt import DbtCliResource, dbt_assets

from dagster_gcp import BigQueryResource

from .project import brazilecom_project

from google.auth import default
from google.cloud import bigquery

import os
import pandas as pd
import pandas_gbq as pdgbq
import kagglehub

# Authenticate the user
# run `gcloud auth application-default login` in your terminal to authenticate using your auth token

# Set the project ID and dataset ID
#bq_project_id = 'dsai-module-2-project' 
bq_project_id = 'premium-node-451703-i2'
bq_dataset_id='brazilecom'

# Use the default credentials
#credentials, project = default()
# Initialize the BigQuery client
#client = bigquery.Client(credentials=credentials, project=bq_project_id)

@asset(compute_kind="python", group_name="Raw_data")
def customer(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.customers"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.
    
@asset(compute_kind="python", group_name="Raw_data")
def sellers(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.sellers"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Raw_data")
def geolocation(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.geolocation"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Raw_data")
def order_items(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.order_items"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Raw_data")
def payments(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.order_payments"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Raw_data")
def order_reviews(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.order_reviews"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Raw_data")
def orders(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.orders"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Raw_data")
def product_translation(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.product_category_name_translation"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Raw_data")
def products(bigquery: BigQueryResource) -> None:
    query=f"select count(*) from {bq_dataset_id}.products"
    with bigquery.get_client() as client:
        query_job=client.query(query)
        result = query_job.result()  # Wait for the query to finish.
    # Log some metadata about the table we just wrote. It will show up in the UI.

@asset(compute_kind="python", group_name="Upstream")
def download_data(context: AssetExecutionContext) -> None:
    # Download latest version
    tables=[]
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce",force_download=True)
    for file_name in os.listdir(path):
        full_file_name = os.path.join(path, file_name)
        if os.path.isfile(full_file_name):
            fname=full_file_name.replace("olist_", "")
            fname=fname.replace("_dataset", "")
            fname = os.path.basename(fname)
            fname = fname.replace(".csv","")
            #print(f"table name={fname}, fullname={bq_dataset_id}.{fname}, project={bq_project_id}")
            data=pd.read_csv(full_file_name)
            
            data=data.iloc[0:1000]
            
            pdgbq.to_gbq(data, f"{bq_dataset_id}.{fname}", project_id=bq_project_id, if_exists="replace")
            tables.append(fname)
    context.add_output_metadata({"num_tables": len(tables), "tables_updated": tables})

#TODO: Check the quaulty 
'''
@asset_check(asset=d)
def missing_dimension_check(context: AssetExecutionContext) -> AssetCheckResult:
    #to check if the dimension table exist.
    df=client.query("select * from dim_customers limit 1").to_dataframe()
    count=df.shape[0]

    return AssetCheckResult(
            passed=count == 0, metadata={"missing dimensions": count}
        )
'''

#TODO: automation for performance report generation
@asset(
    compute_kind="python",
    group_name="Analysis",
    automation_condition=AutomationCondition.eager(),
)
def monthly_sales_performance(
    context: AssetExecutionContext, bigquery: BigQueryResource
):
    query=f"select count(*) from {bq_dataset_id}_facts.facts_orders limit 100"
    with bigquery.get_client() as client:
        df=client.query(query).to_dataframe()
        df_summary_bysellerbyyearbycity = df.groupby(['seller_id', 'order_year','seller_city']).agg({'price': 'sum', 'freight_value': 'sum'}).reset_index()
        df_summary_bysellerbyyearbycity.sort_values(by=['order_year', 'seller_city','price'], ascending=[True, False, True], inplace=True)
        os.makedirs("reports", exist_ok=True)
        df_summary_bysellerbyyearbycity.to_csv("reports/monthly_sales_performance.csv", index=False)
        count=df_summary_bysellerbyyearbycity.shape[0]
        return MaterializeResult(
            metadata={  
                "row_count": MetadataValue.int(count),
                "preview": MetadataValue.md(df_summary_bysellerbyyearbycity.to_markdown(index=False)),
            }
        )

@dbt_assets(manifest=brazilecom_project.manifest_path, name="brazilecom_dbt_assets")
def brazilecom_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_build_args = ["build"]
    yield from dbt.cli(dbt_build_args, context=context).stream()

