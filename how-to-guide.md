# Deployment Guide: (load-gcs-bucket-inventory-to-bigquery)
## 1. Mirror GitHub Source Code to Cloud Source Repository
- Navigate to [Cloud Source Repositories](https://source.cloud.google.com/)
- Click the `+ Add repository` button or navigate to [Add a repository](https://source.cloud.google.com/repo/new) 
- Select `Connect external repository` and click `Continue`
- Select your project, `GitHub` as the `Git provider`, check the authorization and click `Connect to GitHub`
- Authenticate with your GitHub credentials ()
- Select `transformationflow/cloudfunctions` from the repository list and click `Connect selected repository`
-

## 2. Cloud Function Configuration
- Navigate to [Cloud Functions](https://console.cloud.google.com/functions/)
- Click `CREATE FUNCTION`
- Select the following options:

Option | Value
--- |---
Environment | `1st Gen`
Function name | e.g. `load-gcs-bucket-inventory-to-bigquery`
Region | Your desired location
Trigger | `Cloud Pub/Sub`

- Click on `Select a Cloud Pub/Sub topic` and then click `CREATE A TOPIC`
- Add the same Topic ID as the cloudfunctions folder and cloud function name (e.g. `load-gcs-bucket-inventory-to-bigquery`) and click `CREATE TOPIC` and then `SAVE`
- Click on `Runtime, build, connections and security settings`, leave all default options except set the Timeout to 540 seconds and the Maximum number of instances to 100
- Click `NEXT`

## 2. Cloud Function Code
- Set `Runtime` to `Python 3.8`
- Set `Entry point` to `main_function`
- Set `Source code` to `Cloud Source repository`
- Set `Repository` to `github_transformationflow_cloudfunctions`
- Set `Branch name` to `main`
- Set `Directory with source code` to `/load-gcs-bucket-inventory-to-bigquery`
- Click `DEPLOY`.  If it takes a while then deployment is probably going to succeed, the most common error is not being able to find the code. 

## 3. Cloud Scheduler Deployment
- Navigate to [Cloud Scheduler](https://console.cloud.google.com/cloudscheduler) and click `CREATE JOB`
- Name it as e.g. `load-gcs-bucket-inventory-to-bigquery__my_bucket` 
- Set the Region, Frequency using unix-cron (check [this](https://crontab.guru/) for help and Timezone and click `CONTINUE`
- Select `Pub/Sub` as Target type
- Select the Pub/Sub topic defined in the first step
- Add the following attributes:
  - Key 1: `gcs_bucket_uri`
  - Value 1: The uri for your bucket e.g. `gs://my_bucket`
  
  - Key 2: `destination_inventory_table_ref`, 
  - Value 2: The table ref for your inventory table (note that this must already exist with the correct schema.  Create this by executing the `create.empty_gcs_bucket_inventory_table_ddl` flowfunction.
- Click `CONTINUE`, leave the deaults and click `CREATE`

## 4. Cloud Function Testing
- Navigate to the Cloud Function defined in the previous steps
- Click on the `LOGS` tab. Now you are ready to trigger the function.
- In a separate window or tab, navigate to the Cloud Scheduler Job created previously.
- Click on the `RUN NOW` button
- Navigate beck to the Cloud Function logs and check for `Function execution started`

If you receive a 404 Error saying "Table is truncated" then you just need to wait 3-5 minutes and retry.  This is because of issues creating and deleting tables which use streaming, as they have a streaming buffer.  This is the reason the table is not recreated every time.

## 5. Add Bucket Watch Notification
This means that the function will re-run whenever a new object is added to the bucket, keeping the inventory up-to-date at all times.











