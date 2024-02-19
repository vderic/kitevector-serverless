import os

def env_init():
	os.environ['API_USER'] = 'vitesse'
	os.environ['REDIS_HOST'] = 'localhost'

	# the path are generated from index name of the CREATE INDEX, i.e. gs://bucket/db/$index_name and gs://bucket/index/$index_name
	os.environ['INDEX_NAME'] = 'serverless'
	os.environ['DATABASE_URI'] = 'vitesse/db'
	os.environ['INDEX_URI'] = 'vitesse/index'
	os.environ['KV_ROLE'] = 'singleton'   # index-master, index-segment, query-master, query-segment or singleton

	#CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT
	os.environ['CLOUD_RUN_TASK_INDEX'] = '0'
	os.environ['CLOUD_RUN_TASK_COUNT'] = '1'

	os.environ['PORT'] = '8080'

env_init()

from kitevectorserverless.httpd import indexnode

def global_init():
	indexnode.global_init()

global_init()

if __name__ == "__main__":

	indexnode.run()
	
