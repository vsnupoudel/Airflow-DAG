from __future__ import annotations

import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# Define your project ID and dataset ID here
PROJECT_ID = "psychic-cursor-445515-c9"
DATASET_ID = "bq_default"

# Define the SQL query
SQL_QUERY = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.vw_groupByCountryTermYear` AS
SELECT 
    country_name, 
    extracted_year, 
    term, 
    count(`extracted_year`) as counts_under_3,
    count( DISTINCT region_code) as number_of_regions
FROM 
 (
    SELECT 
        country_name, 
        EXTRACT(YEAR FROM `week`) as extracted_year, 
        term,
        region_code,
        ROW_NUMBER() OVER ( PARTITION BY term,country_name, region_code, `week` ORDER BY refresh_date DESC) AS rank_by_date
    FROM `psychic-cursor-445515-c9.bq_default.international_top_rising_terms` 
    WHERE `rank` <= 3
    -- AND country_name LIKE '%Argentina%'
    AND EXTRACT(YEAR FROM `week`) < 2025
    ORDER BY country_name, extracted_year, term, region_code
)AS inwin 
WHERE rank_by_date=1
GROUP BY 
    country_name, 
    extracted_year, 
    term
ORDER BY country_name, extracted_year, term
"""

# Define your destination table (optional, if you want to write the results to a new table)
DESTINATION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.aggregated_top_rising_terms"  # Replace with your desired table name

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),  # Start date for the DAG
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}


# Define the DAG
with models.DAG(
    dag_id="bigquery_withJoin_proper",  # Unique ID for your DAG
    default_args=default_args,
    schedule="@once",  #  Run the DAG once, replace with a cron schedule if needed
    catchup=False, # Prevents backfilling
    tags=["bigquery", "transformation"], # Allows you to organise your DAGs better
) as dag:
    # Define the BigQuery operator
    bigquery_transform = BigQueryExecuteQueryOperator(
        task_id="bigquery_transformation",
        sql=SQL_QUERY,
        use_legacy_sql=False,  # Use standard SQL
        # destination_dataset_table=DESTINATION_TABLE,  # Optional: Specify a destination table
        # write_disposition="WRITE_TRUNCATE",  # Optional: Overwrite the table if it exists.  Consider WRITE_APPEND if you need to append to the existing table.
        gcp_conn_id="google_cloud_default", #Use this if your Composer instance already has the proper access configured with the default service account.  Otherwise, set up a Google Cloud connection in Airflow and point to it here.
    )
    
    load_country_data = GCSToBigQueryOperator(
        task_id='load_country_data',
        bucket=f"{PROJECT_ID}",
        source_objects=['country.txt'],
        destination_project_dataset_table=  f"{PROJECT_ID}.{DATASET_ID}.country_table", #'your-project.your_dataset.country_table',
        schema_fields=[
            {'name': 'country_name', 'type': 'STRING', 'mode':'REQUIRED'},
            {'name': 'importance_ID', 'type': 'INTEGER'},
        ],
        source_format='CSV',
        field_delimiter='\t',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )
    
    join_results = BigQueryExecuteQueryOperator(
        task_id="join_results",
        sql= f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.international_yearly_top3` AS
        SELECT
            a.*,
            b.importance_ID
        FROM
            `{PROJECT_ID}.{DATASET_ID}.vw_groupByCountryTermYear` a
        JOIN
            `{PROJECT_ID}.{DATASET_ID}.country_table` b
        ON
            a.country_name = b.country_name;
        """,
        use_legacy_sql=False,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
    )

    load_country_data >> join_results
    bigquery_transform >> join_results
    
    