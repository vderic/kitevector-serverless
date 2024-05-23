export PORT=8080
export API_USER='vitesse'
export REDIS_HOST='localhost'
export DATABASE_URI='gs://vitesse_deltalake/db'
export INDEX_URI='gs://vitesse_deltalake/index'
export KV_ROLE='singleton'
export CLOUD_RUN_TASK_INDEX=0
export CLOUD_RUN_TASK_COUNT=1
export KV_CONTROL_SERVICE_URI='http://control_service'
export AWS_REGION='us-east-1'
export AWS_ACCESS_KEY_ID='key_id'
export AWS_S3_LOCKING_PROVIDER='dynamodb'
export DELTA_DYNAMO_TABLE_NAME='delta_log'
export GOOGLE_APPLICATION_CREDENTIALS_JSON=`cat ~/.google/aictrl.json`
export INDEX_JSON='{"name":"serverless",
    "dimension" : 1536,
    "metric_type" : "ip",
    "schema": { "fields" : [{"name": "id", "type":"int64", "is_primary": "true"},
        {"name":"vector", "type":"vector"},
        {"name":"animal", "type":"string"}
        ]},
    "params": {"max_elements" : 1000, "ef_construction":48, "M": 24}
    }'

#echo $GOOGLE_APPLICATION_CREDENTIALS_JSON
#echo $INDEX_JSON

docker run --expose $PORT -p $PORT:$PORT -e PORT \
        -e API_USER \
        -e REDIS_HOST \
        -e DATABASE_URI \
        -e INDEX_URI \
        -e KV_ROLE \
        -e CLOUD_RUN_TASK_INDEX \
        -e CLOUD_RUN_TASK_COUNT \
        -e KV_CONTROL_SERVICE_URI \
        -e AWS_REGION \
        -e AWS_ACCESS_KEY_ID \
        -e AWS_S3_LOCKING_PROVIDER \
        -e DELTA_DYNAMO_TABLE_NAME \
        -e GOOGLE_APPLICATION_CREDENTIALS_JSON \
        -e INDEX_JSON \
        --name standalone-ai kitevector:v1

