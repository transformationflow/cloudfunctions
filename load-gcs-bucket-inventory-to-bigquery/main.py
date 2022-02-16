import base64
import json
import os

from google.cloud import storage
from google.cloud import bigquery

def main_function(event, context=None):
    try:
        # get attributes
        gcs_bucket_uri = event['attributes']['gcs_bucket_uri']
        destination_inventory_table_ref = event['attributes']['destination_inventory_table_ref']
        gcs_bucket_name = gcs_bucket_uri.replace("gs://", "")

        # get project_id from environment variable and initialise storage/bigquery clients
        project_id = os.environ.get('GCP_PROJECT')
        GCS = storage.Client(project=project_id)
        BQ = bigquery.Client(project=project_id)

        # get objects in bucket
        bucket_objects = GCS.list_blobs(gcs_bucket_name)
        #bucket_objects_list = list(bucket_objects)

        # truncate BQ table
        truncate_table_query = f"TRUNCATE TABLE `{destination_inventory_table_ref}´"
        result = BQ.query(query=truncate_table_query)
        table = BQ.get_table(destination_inventory_table_ref)

        # stream object properties into empty BQ table
        error_count = 0
        errors_list = []
        for bucket_object in bucket_objects:
            object_properties = bucket_object._properties
            
            # json encode optional object metadata
            if "metadata" in object_properties:
                object_properties["metadata"] = json.dumps([{"key": k, "value": v} for k, v in object_properties["metadata"].items()])
            else:
                object_properties["metadata"] = None
            
            rows_to_insert = [object_properties]

            errors = BQ.insert_rows(
                table, rows_to_insert, row_ids=[None] * len(rows_to_insert)
            )
            
            if len(errors) > 0:
                print(f"error loading file information: {object_properties['bucket']}/{object_properties['name']}")
                error_count += 1
                errors_list.append(object_properties['name'])

            else:    
                print(f"success loading file information: {object_properties['bucket']}/{object_properties['name']}") 
            break

    except Exception as e:
        print("Exception:", e)   


# LOCAL TESTING
if os.environ.get('ENVIRONMENT_TYPE')  == "TEST":
# set test event attributes and call main_function
    event_a = dict()
    event_a['attributes'] = {
        'gcs_bucket_uri': 'gs://gdbm-public',
        'destination_inventory_table_ref': 'miles-partnership-6751.dv360_sdf_entity_lookups_admin.gcs_bucket_inventory'
        }
    event = dict()
    event['attributes'] = {
        'gcs_bucket_uri': 'dl-mp-centro',
        'destination_inventory_table_ref': 'miles-partnership-6751.centro_admin.gcs_bucket_inventory'
        }

    main_function(event)