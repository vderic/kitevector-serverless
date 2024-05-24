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

cat <<EOF > pod.yaml

apiVersion: v1
kind: Pod
metadata:
  name: kitevector
spec:
  containers:
    - name: kitevector
      image: kitevector:v1
      imagePullPolicy: Never
      ports:
        - containerPort: 8080
      env:
        - name: PORT
          value: "8080"
        - name: API_USER
          value: "vitesse"
        - name: REDIS_HOST
          value: "localhost"
        - name: DATABASE_URI
          value: "gs://vitesse_deltalake/db"
        - name: INDEX_URI
          value: "gs://vitesse_deltalake/index"
        - name: KV_ROLE
          value: "singleton"
        - name: CLOUD_RUN_TASK_INDEX
          value: "0"
        - name: CLOUD_RUN_TASK_COUNT
          value: "1"
        - name: KV_CONTROL_SERVICE_URI
          value: "http://control_service"
        - name: AWS_REGION
          value: "us-east-1"
        - name: AWS_ACCESS_KEY_ID
          value: "keyid"
        - name: AWS_SECRET_ACCESS_KEY
          value: "secret_key"
        - name: AWS_S3_LOCKING_PROVIDER
          value: "dynamodb"
        - name: DELTA_DYNAMO_TABLE_NAME
          value: "delta_log"
        - name: GOOGLE_APPLICATION_CREDENTIALS_JSON
          value: '${GOOGLE_APPLICATION_CREDENTIALS_JSON}'
        - name: INDEX_JSON
          value: '${INDEX_JSON}'

EOF

cat <<EOF > deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  creationTimestamp: null
  labels:
    app: kitevector
  name: kitevector
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kitevector
  strategy: {}
  template:
    metadata:
      creationTimestamp: null
      labels:
        app: kitevector
    spec:
      containers:
      - image: kitevector:v1
        name: kitevector
        imagePullPolicy: Never
        resources: {}
        ports:
          - containerPort: 8080
        env:
          - name: PORT
            value: "8080"
          - name: API_USER
            value: "vitesse"
          - name: REDIS_HOST
            value: "localhost"
          - name: DATABASE_URI
            value: "gs://vitesse_deltalake/db"
          - name: INDEX_URI
            value: "gs://vitesse_deltalake/index"
          - name: KV_ROLE
            value: "singleton"
          - name: CLOUD_RUN_TASK_INDEX
            value: "0"
          - name: CLOUD_RUN_TASK_COUNT
            value: "1"
          - name: KV_CONTROL_SERVICE_URI
            value: "http://control_service"
          - name: AWS_REGION
            value: "us-east-1"
          - name: AWS_ACCESS_KEY_ID
            value: "keyid"
          - name: AWS_SECRET_ACCESS_KEY
            value: "secret_key"
          - name: AWS_S3_LOCKING_PROVIDER
            value: "dynamodb"
          - name: DELTA_DYNAMO_TABLE_NAME
            value: "delta_log"
          - name: GOOGLE_APPLICATION_CREDENTIALS_JSON
            value: '${GOOGLE_APPLICATION_CREDENTIALS_JSON}'
          - name: INDEX_JSON
            value: '${INDEX_JSON}'
status: {}
EOF


kubectl apply -f deployment.yaml

kubectl expose deployment kitevector --type=NodePort --port=8080

# minikube service kitevector
# kubectl describe pods -l app=kitevector

# to delete deployment and service
# kubectl delete -f deployment.yaml
# kubectl delete service kitevector

