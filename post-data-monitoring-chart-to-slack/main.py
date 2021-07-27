import altair as alt
import google.auth
import os
import pandas as pd
import requests
import sys

from altair_saver import save
from datetime import datetime
from google.cloud import bigquery, secretmanager
from pprint import pprint
from selenium import webdriver

import cairosvg


def main_function(event, context=None):
    try:
 
        # monitoring chart path
        chart_path = 'tmp/inbound_data_monitoring.svg'
        chart_path_png = 'tmp/inbound_data_monitoring.png'

        # get current datetime for slack post title
        now = datetime.now()
        now_string = now.strftime("%Y-%m-%d %H:%M:%S")

        # get slack attributes from scheduler payload
        slack_access_token_name = event['attributes']['slack_access_token_name']
        slack_channel = event['attributes']['slack_channel']

        # get query attributes from scheduler payload
        inbound_monitoring_dataset_ref = event['attributes']['inbound_monitoring_dataset_ref']
        ingest_timestamp_column = event['attributes']['ingest_timestamp_column']
        table_exclusion_clause = event['attributes']['table_exclusion_clause']
        specific_table_exclusions = event['attributes']['specific_table_exclusions']
        days_to_display = event['attributes']['days_to_display']

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
        AND table_name NOT IN ({','.join(f"'{table}'" for table in specific_table_exclusions)})
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

        SELECT initial_query_sql;
        """

        inbound_monitoring_response = BQ.query(query=ingest_sql)
        inbound_monitoring_response_dict = [dict(row) for row in inbound_monitoring_response]
        inbound_monitoring_sql = inbound_monitoring_response_dict[0]['initial_query_sql']

        clean_gapless_monitoring_query = f"""
        WITH 
        ingest_records 
        AS (
        {inbound_monitoring_sql}
        ),

        date_array AS (
        SELECT GENERATE_DATE_ARRAY(
        (SELECT MIN(ingest_date) FROM ingest_records), 
        (SELECT MAX(ingest_date) FROM ingest_records)) AS all_dates
        ),

        date_array_flat AS (
        SELECT ingest_date 
        FROM date_array, UNNEST (all_dates) AS ingest_date
        ),

        unique_tables AS (
        SELECT DISTINCT table_name
        FROM ingest_records
        ),

        date_table_scaffold AS (
        SELECT * 
        FROM unique_tables CROSS JOIN date_array_flat
        ),

        gapless_monitoring_data AS (
        SELECT * FROM date_table_scaffold
        LEFT JOIN ingest_records USING (table_name, ingest_date)
        ),

        gapless_monitoring_data_with_zeroes AS (
        SELECT 
        table_name, 
        ingest_date, 
        IFNULL(ingest_records, 0) AS ingest_records, 
        IFNULL(ingest_records_flag, 0) AS ingest_records_flag 
        FROM gapless_monitoring_data
        )


        SELECT * FROM gapless_monitoring_data_with_zeroes
        ORDER BY ingest_date DESC, table_name
        """

        response = BQ.query(query=clean_gapless_monitoring_query)
        monitoring_df = response.to_dataframe()
        monitoring_df["ingest_date"] = pd.to_datetime(monitoring_df["ingest_date"])

        date_filtered_monitoring_df = monitoring_df[monitoring_df.ingest_date > datetime.now() - pd.to_timedelta(f"{days_to_display}day")]

        # build chart and save to /tmp directory
        inbound_monitoring_chart = alt.Chart(date_filtered_monitoring_df).mark_rect().encode(
            x = alt.X('ingest_date:T', axis=alt.Axis(title='Ingest Date', labelAngle=-90, format="%d %b", tickCount=date_filtered_monitoring_df["ingest_date"].nunique()), sort='-x'),
            y = alt.Y('table_name:O', title='Table Name', sort=alt.Sort(field='table_name', order='ascending')),
            color=alt.Color('ingest_records_flag:Q', scale = alt.Scale(domain=[0,1], range = ['#67001f','#006837'], type='ordinal'), legend=None),
            tooltip = [alt.Tooltip('table_name:O', title='Table Name'),
                    alt.Tooltip('ingest_date:T', title='Ingested Date'),
                    alt.Tooltip('ingest_records:Q', title='Records Ingested'),               
                    alt.Tooltip('ingest_records_flag:Q', title='Any Records Flag')
                    ]
        ).configure_scale(rectBandPaddingInner=0.1
        ).properties(width=900, title=f'Inbound Data Monitoring: {inbound_monitoring_dataset_ref}'
        ).interactive()

        print("Using Chrome Webdriver")
        # attempted the following to set path:
        #sys.path.append('/Users/JimBarlow/transformationflow/cloudfunctions/post-data-monitoring-chart-to-slack/chromedriver.exe')
        #export PATH="$PATH:/Users/JimBarlow/transformationflow/cloudfunctions/post-data-monitoring-chart-to-slack/chromedriver.exe"
        
        # PATHs
        ABSPATH = os.path.abspath(__file__) # e.g: c:/root_folder/main.py
        DIRPATH = os.path.dirname(ABSPATH) # e.g: c:/root_folder/
        TMP_DIR_PATH = os.path.join(DIRPATH, "tmp") # create "tmp" folder path. e.g: c:/root_folder/tmp
        MAC64_DIR_PATH = os.path.join(TMP_DIR_PATH, "mac64")
        CHROMEDRIVER_FILE_PATH = os.path.join(MAC64_DIR_PATH, "chromedriver.exe")
        
        #chrome_webdriver = webdriver.Chrome(executable_path=r"/tmp/mac64/chromedriver.exe", service_log_path="/tmp")
        chrome_webdriver = webdriver.Chrome(executable_path=CHROMEDRIVER_FILE_PATH, service_log_path=TMP_DIR_PATH)
        save(inbound_monitoring_chart, chart_path, webdriver=chrome_webdriver)
        

        # get slack access token from secret manager
        secret_name = f"projects/{project}/secrets/{slack_access_token_name}/versions/latest"        
        SM = secretmanager.SecretManagerServiceClient()
        secret_response = SM.access_secret_version(name=secret_name)
        slack_access_token = secret_response.payload.data.decode("UTF-8")

        # convert SVG chart to PNG
        output = cairosvg.svg2png(url=chart_path, write_to=chart_path_png)

        with open(chart_path_png, "rb") as f:
            file_bytes = f.read()

        # send response to slack channel
        slack_payload = {
            'token': slack_access_token,
            'channels': slack_channel,
            'filename': 'inbound_data_monitoring',
            'filetype': 'png',
            'initial_comment': f'{inbound_monitoring_dataset_ref}',
            'title': f'Observation time: {now_string}'
        }

        slack_response = requests.post('https://slack.com/api/files.upload', slack_payload, files = { 'file': file_bytes }).json()
        pprint(slack_response)

    except Exception as e:
        print("ERROR:", e)       


# function execution for local test environment

if os.environ.get('ENVIRONMENT_TYPE')  == "TEST":

    test_event = {'attributes': {
        'inbound_monitoring_dataset_ref': 'tripscout-151203.airbyte_instagram_sync',
        'table_exclusion_clause': '_airbyte%',
        'specific_table_exclusions': ['media_0f8_owner', 'media_children', 'media_children_4f7_owner', 'media_children_owner', 'media_d39_children', 'stories_ce2_owner', 'stories_owner'],
        'ingest_timestamp_column': '_airbyte_emitted_at',
        'slack_access_token_name': 'beepbeep_data_monitor',
        'slack_channel': '#ig-monitoring-dev',
        'days_to_display': 60
        }} 



    main_function(test_event)

    # 'slack_access_token_name': 'beepbeep_data_monitor', 
    # 'slack_channel': '#ig-monitoring-dev',

    # 'slack_access_token_name': 'slack-data-monitor',
    # 'slack_channel': '#ig-alerts',

