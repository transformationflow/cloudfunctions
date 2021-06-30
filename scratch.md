# Scratch file with useful code snippets

### Example gsutil to force deployment of latest commit
```
gcloud functions deploy post-bigquery-response-to-slack --source=https://source.developers.google.com/projects/tripscout-151203/repos/github_transformationflow_cloudfunctions/revisions/[REVISION_ID]/paths/cloudfunctions/post-bigquery-response-to-slack
```
