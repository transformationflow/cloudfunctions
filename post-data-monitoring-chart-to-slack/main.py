# placeholder for chart generation and slack channel post code
import os
import pandas as pd
import altair as alt

import google.auth
from google.cloud import bigquery, secretmanager


def main_function(event, context=None):
    try:
 
        # get slack attributes from scheduler payload
        slack_access_token_name = event['attributes']['slack_access_token_name']
        slack_channel = event['attributes']['slack_channel']

        # get query attributes from scheduler payload
        inbound_monitoring_dataset_ref = event['attributes']['inbound_monitoring_dataset_ref']
        ingest_timestamp_column = event['attributes']['ingest_timestamp_column']
        table_exclusion_clause = event['attributes']['table_exclusion_clause']

        # construct bigquery client with drive scopes (for accessing federated gsheet tables)
        credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"]
        )
        BQ = bigquery.Client(credentials=credentials, project=project)

        # construct query for inbound data monitoring
        ingest_sql = f"""
        DECLARE initial_query_sql STRING;

        SET initial_query_sql = (
        WITH 
        all_table_info_schema AS (
        SELECT * FROM `{inbound_monitoring_dataset_ref}`.INFORMATION_SCHEMA.TABLES
        WHERE table_name NOT LIKE '{table_exclusion_clause}'
        ),

        all_table_refs AS (
        SELECT 
        CONCAT(table_catalog, ".", table_schema, ".", table_name) AS table_ref,
        table_name
        FROM all_table_info_schema
        ),

        table_level_sql AS (
        SELECT FORMAT("SELECT '%s' AS table_name, EXTRACT (DATE FROM {ingest_timestamp_column}) AS ingest_date, COUNT(*) AS ingest_records, CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS ingest_records_flag FROM `%s` GROUP BY ingest_date", table_name, table_ref) AS table_sql 
        FROM all_table_refs
        ),

        sql_out AS (
        SELECT ARRAY_TO_STRING(ARRAY_AGG(table_sql), '\\nUNION ALL\\n') AS sql_string
        FROM table_level_sql
        )

        SELECT * 
        FROM sql_out
        );


        EXECUTE IMMEDIATE (initial_query_sql);
        """

        response = BQ.query(query=ingest_sql)
        monitoring_df = response.to_dataframe()
        monitoring_df["ingest_date"] = pd.to_datetime(monitoring_df["ingest_date"])


        


    except Exception as e:
        print("ERROR:", e)       



# function execution for local test environment
if os.environ.get('ENVIRONMENT_TYPE')  == "TEST":

    test_event = {'attributes': {
        'inbound_monitoring_dataset_ref': 'tripscout-151203.airbyte_instagram_sync',
        'table_exclusion_clause': '_airbyte%',
        'ingest_timestamp_column': '_airbyte_emitted_at',
        'slack_access_token_name': 'beepbeep_data_monitor',
        'slack_channel': '#ig-monitoring'
        }} 

    main_function(test_event)