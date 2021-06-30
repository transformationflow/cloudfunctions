import base64
import json
import os

# main function
def main_function(event, context=None):
    try:
        payload = base64.b64decode(event['data']).decode('utf-8')
        print("payload:", payload )

        print(event['attributes'])

    except Exception as e:
        print(e)     


# function execution for local test environment
if os.environ.get('ENVIRONMENT_TYPE')  == "TEST":
    
    scheduler_payload_dict = {  "slack_channel": "notify-tripscout-igmentions",
                                "slack_access_token": "xoxb-348972820896-1105493528022-knl73IGnbTslsgZdMRGUNsZa",
                                "sql_query": """
                                SELECT 
                                ARRAY_TO_STRING(ARRAY_AGG(
                                FORMAT('ALERT: %d violations and %d potential violations from %d posts on %t', 
                                requested_count + has_other_permissions_count + not_requested_count + denied_count,
                                one_image_permitted_count,
                                all_posts,
                                post_date)
                                ), '\n')
                                FROM `tripscout-151203.instagram_insights._bi_user_mention_permission_status_summary`
                                WHERE post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) 
                                """}
    scheduler_payload_json = json.dumps(scheduler_payload_dict)

    print(scheduler_payload_dict)
#   main_function(event)
