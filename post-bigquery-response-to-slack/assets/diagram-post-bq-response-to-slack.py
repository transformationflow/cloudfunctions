# requirement: pip install diagrams

from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery, PubSub
from diagrams.gcp.compute import Functions
from diagrams.gcp.devtools import Scheduler, SourceRepositories
from diagrams.onprem.vcs import Github
from diagrams.saas.chat import Slack
from diagrams.gcp.security import KeyManagementService
from diagrams.programming.flowchart import Document

graph_attr = {"pad": "0", "fontsize": "24", "bgcolor": "white"}

with Diagram("Post BigQuery Response to Slack Channel", direction="LR", outformat = "png", filename='post-bq-response-to-slack', graph_attr=graph_attr) as diag:

  with Cluster("Configuration & Trigger"):
    CG = Document("Configuration")
    CS = Scheduler("Cloud Scheduler")
    PS = PubSub("PubSub")

    CG >> Edge(label='1. Config set in scheduler attributes\n(schedule, query, slack channel and key name)') >> CS
    CS >> Edge(label='2. Config sent in payload attributes') >> PS

  with Cluster("Execution"):
    CF = Functions("Cloud Function")
    
    PS >> Edge(label='3. Function triggered with attributes fom config') >> CF

  with Cluster("Data Warehouse"):
    BQ = BigQuery("BigQuery")

    BQ << Edge(label='4. Query executed and response received') >> CF

  with Cluster("Notification"):
    KM = KeyManagementService("Secret Manager")
    SL = Slack("Slack Channel")

    CF << Edge(label='5. Slack access token obtained from Secret Manager') >> KM
    CF >> Edge(label='5. Query result posted to Slack Channel') >> SL

diag