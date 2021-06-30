import base64
import json
import os

import google.auth
from google.cloud import bigquery


# main function
def main_function(event, context=None):
    try:
 
        # get attributes from scheduler payload and set variables
        slack_access_token = event['attributes']['slack_access_token']
        slack_channel = event['attributes']['slack_channel']
        sql_query = event['attributes']['sql_query']
        print(f"posting to {slack_channel} response from {sql_query}")

        # execute bigquery query and print response
        project_id = os.environ.get('GCP_PROJECT')
        #credentials = google.auth.default(
        #scopes=[
        #    "https://www.googleapis.com/auth/cloud-platform",
        #    "https://www.googleapis.com/auth/drive",
        #    "https://www.googleapis.com/auth/bigquery"]
        #)
        BQ = bigquery.Client(project=project_id)#, credentials=credentials)
        print(dir(BQ))

        sql_query_clean = sql_query.replace("\n", " ")
        response = BQ.query(query=sql_query_clean).to_dataframe().to_dict(orient='records')
        
        print(type(response), response)
        
        # tbc get secret from secret manager



        # send response to slack channel



    except Exception as e:
        print(e)     

test_event = {'attributes': {'slack_access_token': 'xoxb-348972820896-1105493528022-knl73IGnbTslsgZdMRGUNsZa',
'slack_channel': 'notify-tripscout-igmentions',
'sql_query': """
SELECT ARRAY_TO_STRING(ARRAY_AGG(FORMAT(
    'ALERT: %d violations and %d potential violations from %d posts on %t',  
    requested_count + has_other_permissions_count + not_requested_count + denied_count, one_image_permitted_count, all_posts, post_date)
    ), '\n') 
FROM `tripscout-151203.instagram_insights._bi_user_mention_permission_status_summary` 
WHERE post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) 
"""}} 

# function execution for local test environment
if os.environ.get('ENVIRONMENT_TYPE')  == "TEST":
    main_function(test_event)