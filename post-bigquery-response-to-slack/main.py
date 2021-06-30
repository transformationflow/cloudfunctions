import base64
import json
import os
import requests

import google.auth
from google.cloud import bigquery, secretmanager
from pprint import pprint

def main_function(event, context=None):
    try:
 
        # get attributes from scheduler payload and log variable values
        slack_access_token_name = event['attributes']['slack_access_token_name']
        slack_channel = event['attributes']['slack_channel']
        sql_query = event['attributes']['sql_query']
        
        print('slack_access_token_name:', slack_access_token_name)
        print('slack_channel:', slack_channel)
        print('sql_query:')
        print(sql_query)
        
        # construct bigquery client with drive scopes (for accessing federated gsheet tables)
        credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"]
        )
        BQ = bigquery.Client(credentials=credentials, project=project)

        # execute bigquery query and set post_text from a string of the first row of response
        sql_query_clean = sql_query.replace("\n", " ")
        response = BQ.query(query=sql_query_clean)
        response_list = [dict(row) for row in response]
        print('response_list:')
        print(response_list)
        
        post_text = str(response_list[0]['post_text'])
        print("post_text:")
        print(post_text) 

        # get slack access token from secret manager
        secret_name = f"projects/{project}/secrets/{slack_access_token_name}/versions/latest"        
        SM = secretmanager.SecretManagerServiceClient()
        secret_response = SM.access_secret_version(name=secret_name)
        slack_access_token = secret_response.payload.data.decode("UTF-8")

        # send response to slack channel
        slack_payload = {
            'token': slack_access_token,
            'channel': slack_channel,
            'text': post_text
        }
        
        slack_response = requests.post('https://slack.com/api/chat.postMessage', slack_payload).json()
        pprint(slack_response)

    except Exception as e:
        print("ERROR:", e)     


# function execution for local test environment
if os.environ.get('ENVIRONMENT_TYPE')  == "TEST":

    test_event = {'attributes': {
        'slack_access_token_name': 'slack-data-monitor',
        'slack_channel': '#notify-tripscout-igmentions',
        'sql_query': """
    SELECT ARRAY_TO_STRING(ARRAY_AGG(FORMAT(
    'ALERT: %d violation(s) and %d potential violation(s) from %d posts on %t',  
    requested_count + has_other_permissions_count + not_requested_count + denied_count, one_image_permitted_count, all_posts, post_date)
    ), '\\n') AS post_text
    FROM `tripscout-151203.instagram_insights._bi_user_mention_permission_status_summary` 
    WHERE post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) 
    """}} 

    main_function(test_event)