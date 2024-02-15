import os
import time
import sys
import heapq
import numpy as np
import pyarrow as pa
import math
import random
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError, TableNotFoundError, CommitFailedError
from kitevectorserverless import index,db

def global_init():
	os.environ['API_USER'] = 'vitesse'
	os.environ['REDIS_HOST'] = 'localhost'

	# the path are generated from index name of the CREATE INDEX, i.e. gs://bucket/db/$index_name and gs://bucket/index/$index_name
	os.environ['INDEX_NAME'] = 'serverless'
	os.environ['DATABASE_URI'] = 'vitesse/db'
	os.environ['INDEX_HOME'] = 'vitesse/index'
	os.environ['KV_ROLE'] = 'singleton'   # master, segment or singleton

	#CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT
	os.environ['CLOUD_RUN_TASK_INDEX'] = '0'
	os.environ['CLOUD_RUN_TASK_COUNT'] = '1'

	fragid = int(os.environ.get('CLOUD_RUN_TASK_INDEX'))
	fragcnt = int(os.environ.get('CLOUD_RUN_TASK_COUNT'))

	indexhome = os.environ.get('INDEX_HOME')
	db_uri = os.environ.get('DATABASE_URI')

	# should be initialize here 
	index_name = os.environ.get('INDEX_NAME')
	db_storage_options = None
	role = os.environ.get('KV_ROLE')
	redis_host = os.environ.get('REDIS_HOST')
	user = os.environ.get('API_USER')

	# global init index. If there is a index file indexdir/$fragid.hnsw found, load the index file into the memory
	index.Index.init(index=index_name, fragid=fragid, index_uri=indexhome, db_uri=db_uri, storage_options=db_storage_options, redis=redis_host, role=role, user=user)
	

def gen_embedding(nitem):
	ret = []
	for x in range(nitem):
		ret.append(random.uniform(-1,1))
	sum = 0
	for x in ret:
		sum += x*x
	sum = math.sqrt(sum)
	# normalize
	for i in range(len(ret)):
		ret[i] = ret[i] / sum
	return ret


global_init()

if __name__ == '__main__':
	try:

		random.seed(1000)

		index_dict = {'schema': {'fields': [ {'name': 'id', 'is_primary': True, 'type': 'int64'},
									{'name':'vector', 'type': 'vector'},
									{'name':'animal', 'type': 'string'}]
								},
						'dimension': 1536,
						'metric_type': 'ip',
						'name': 'serverless-index',
						'params' : { 'max_elements' : 1000, 'ef_construction' : 48, 'M' : 24}
						}

		vectors = []
		for i in range(4):
			vectors.append(gen_embedding(index_dict['dimension']))
			
		#print(vectors)

		data = {'id':[1,2,3,4], 
				'vector': vectors,
				'animal':['tiger', 'fox', 'frog', 'cat']}

		created = False

		if created:
			index.Index.insertData(data)
		else:
			index.Index.create(index_dict)

		#time.sleep(0.1)
		#dt = db.KVDeltaTable(os.environ.get('DATABASE_HOME'), index_dict['schema'])
		#df = dt.select(['vector', 'id']).filter(db.OpExpr('=', 'animal', 'fox')).filter(db.ScalarArrayOpExpr('id', [2,3])).execute()
		#df = dt.to_pandas(columns=['vector'], filters=[('id', 'in', [2,3]), ('animal', '=', 'apple')])

		#print(df.to_string())

		#dt.delete()
		#time.sleep(0.1)
	except DeltaError as e:
		print(e)
