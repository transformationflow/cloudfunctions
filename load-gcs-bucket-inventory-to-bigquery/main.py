import base64
import json
import os
from datetime import datetime


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

        # get file names and hashes currently in inventory
        get_current_inventory_query = f"SELECT name, md5Hash FROM `{destination_inventory_table_ref}`"
        result = BQ.query(query=get_current_inventory_query)
        current_inventory_names = [row['name'] for row in result]
        current_inventory_hashes = {row['name']: row['md5Hash'] for row in result}

        table = BQ.get_table(destination_inventory_table_ref)

        for bucket_object in bucket_objects:
            object_properties = bucket_object._properties
            
            # json encode optional object metadata
            if "metadata" in object_properties:
                object_properties["metadata"] = json.dumps([{"key": k, "value": v} for k, v in object_properties["metadata"].items()])
            else:
                object_properties["metadata"] = None

            # if file name does not exist then stream row to bq
            if object_properties['name'] not in current_inventory_names:
                #print(object_properties['name'])
                errors = BQ.insert_rows(table, [object_properties], row_ids=[None] * len(object_properties))
                #print(f"streamed with {len(errors)} errors {object_properties['name']}. errors: {errors}:")

            # if file name exists and hash is different then stream row to bq
            elif object_properties['name'] in current_inventory_names and current_inventory_hashes[object_properties['name']] != object_properties['md5Hash']:
                #print(object_properties['name'], ": ", object_properties['md5Hash'])
                errors = BQ.insert_rows(table, [object_properties], row_ids=[None] * len(object_properties))
                #print(f"streamed updated info with {len(errors)} errors: {object_properties['name']}. errors: {errors}")

            #else:
                #print(f"file with matching hash {current_inventory_hashes[object_properties['name']]} already exists: {object_properties['name']} ")



            
            
            

                


        # if file name exists and hash is the same, do nothing



        # truncate BQ table
        # truncate_table_query = f"TRUNCATE TABLE `{destination_inventory_table_ref}´"
        #truncate_table_query = f"EXECUTE IMMEDIATE (SELECT `flowus.create.empty_gcs_bucket_inventory_table_ddl`('{destination_inventory_table_ref}'));"
        #print(truncate_table_query)
        #result = BQ.query(query=truncate_table_query)
        #print(result)
        #print(type(result))
        #print(dir(result))
        #print(result.state)

        #table = BQ.get_table(destination_inventory_table_ref)

        # stream object properties into empty BQ table
        #error_count = 0
        #errors_list = []
        #all_rows = []
       # for bucket_object in bucket_objects:
            #object_properties = bucket_object._properties
            
            # json encode optional object metadata
            #if "metadata" in object_properties:
            #    object_properties["metadata"] = json.dumps([{"key": k, "value": v} for k, v in object_properties["metadata"].items()])
            #else:
            #    object_properties["metadata"] = None
            
            #rows_to_insert = [object_properties]
            #all_rows.append(object_properties)
            #errors = BQ.insert_rows(
            #    table, rows_to_insert, row_ids=[None] * len(rows_to_insert)
            #)
        #print(f"{len(all_rows)} rows to be loaded in 5000 row sets")

        #def divide_chunks(l, n):
        #     for i in range(0, len(l), n): 
        #        yield l[i:i + n]

        # divide list into chunks of 5000
        # all_rows_chunked = divide_chunks(all_rows, 5000)

        # load chunks to bigquery
        # for loading_rows in all_rows_chunked:
        #    errors = BQ.insert_rows(table, loading_rows, row_ids=[None] * len(loading_rows))
        #    print(f"errors: {errors}")


        #print(all_rows)


        #    if len(errors) > 0:
        #        print(f"error loading file information: {object_properties['bucket']}/{object_properties['name']}")
        #        error_count += 1
        #        errors_list.append(object_properties['name'])

        #    else:    
        #        print(f"success loading file information: {object_properties['bucket']}/{object_properties['name']}") 
            
            
            

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