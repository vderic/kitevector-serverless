import os

def env_init():
	os.environ['API_USER'] = 'vitesse'
	os.environ['REDIS_HOST'] = 'localhost'

	# the path are generated from index name of the CREATE INDEX, 
	# i.e. gs://bucket/db/$index_name/$namespace and gs://bucket/index/$index_name/$namespace
	os.environ['INDEX_NAME'] = 'serverless'
	os.environ['DATABASE_URI'] = 'vitesse/db'
	os.environ['INDEX_URI'] = 'vitesse/index'
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
	os.environ['GOOGLE_SERVICE_ACCOUNT'] = 'account'
	os.environ['GOOGLE_SERVICE_ACCOUNT_KEY'] = 'key'
	os.environ['GOOGLE_BUCKET'] = 'bucket'


	os.environ['PORT'] = '8080'

env_init()

from kitevectorserverless.httpd import indexnode

def global_init():
	indexnode.global_init()

global_init()

if __name__ == "__main__":

	indexnode.run()
	
