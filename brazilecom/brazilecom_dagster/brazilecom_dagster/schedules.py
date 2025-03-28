"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition, AssetSelection
from dagster_dbt import build_schedule_from_dbt_selection
from .assets import brazilecom_dbt_assets


schedules = [
     build_schedule_from_dbt_selection(
        [brazilecom_dbt_assets],
        job_name="DBT_Transformation_Job",
        execution_timezone="Asia/Singapore",
        cron_schedule="0 2 * * *",
        dbt_select="fqn:*"
    ),
    ScheduleDefinition(
        name="Extraction_and_Looading_Job",
        target=AssetSelection.keys("download_data"),
        execution_timezone="Asia/Singapore",
        cron_schedule="0 0 * * *",  # every Monday at midnight
    ),
    ScheduleDefinition(
        name="Perform_Analysis_Job",
        target=AssetSelection.keys("monthly_sales_performance"),
        execution_timezone="Asia/Singapore",
        cron_schedule="0 4 * * *",  # every Monday at midnight
    ),
]

