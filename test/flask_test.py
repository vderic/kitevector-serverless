import os
import sys
import subprocess

def env_init():
    os.environ['API_USER'] = 'vitesse'
    os.environ['REDIS_HOST'] = 'localhost'

    # the path are generated from index name of the CREATE INDEX, 
    # i.e. gs://bucket/db/$index_name/$namespace and gs://bucket/index/$index_name/$namespace
    os.environ['DATABASE_URI'] = 'gs://vitesse_deltalake/db'
    os.environ['INDEX_URI'] = 'gs://vitesse_deltalake/index'
    os.environ['KV_ROLE'] = 'singleton'   # index-master, index-segment, query-master, query-segment or singleton

    #CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT
    os.environ['CLOUD_RUN_TASK_INDEX'] = '0'
    os.environ['CLOUD_RUN_TASK_COUNT'] = '1'

    # KV_CONTROL_SERVICE_URI
    os.environ['KV_CONTROL_SERVICE_URI'] = "http://control_service"


    # delta storage option
    # For AWS,
    # storage_options = {"region": "region", "AWS_ACCESS_KEY_ID": "abc", "AWS_SECRET_ACCESS_KEY":"dfg", 
    # 'AWS_S3_LOCKING_PROVIDER': 'dynamodb', 'DELTA_DYNAMO_TABLE_NAME': 'delta_log'}
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['AWS_ACCESS_KEY_ID' ] ='key_id'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret_key'
    os.environ['AWS_S3_LOCKING_PROVIDER'] = 'dynamodb'
    os.environ['DELTA_DYNAMO_TABLE_NAME'] = 'delta_log'

    # For GCS,
    with open(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')) as f:
        gcp_secret = f.read()
        os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'] = gcp_secret

    os.environ['PORT'] = '8080'
    os.environ['PYTHONPATH']=''

    # JSON
    os.environ['INDEX_JSON']='''{"name":"serverless",
    "dimension" : 1536,
    "metric_type" : "ip",
    "schema": { "fields" : [{"name": "id", "type":"int64", "is_primary": "true"},
        {"name":"vector", "type":"vector"},
        {"name":"animal", "type":"string"}
        ]},
    "params": {"max_elements" : 1000, "ef_construction":48, "M": 24}
    }'''


env_init()

from kitevectorserverless.httpd import indexnode

if __name__ == "__main__":

    try:
#       # in Dockerfile
#       #CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 kitevectorserverless.httpd.indexnode:app
#       subprocess.run(['gunicorn', '--workers', '1', '--threads', '8', 
#           '--timeout', '0', 'kitevectorserverless.httpd.indexnode:app'])

        indexnode.run(debug=True)

    except KeyboardInterrupt:
        sys.exit(0)
    
