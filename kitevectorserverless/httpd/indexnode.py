import numpy as np
import json
import os
import threading
from werkzeug.exceptions import HTTPException, BadRequest
from flask import Flask, request, json, abort, jsonify
from kitevectorserverless.index import Index
from kitevectorserverless.datatype import IndexConfig
from operator import itemgetter

# Flask initialization
app = Flask(__name__)

@app.errorhandler(BadRequest)
def handler_bad_request(e):
	response = {'code': 400, 'message': str(e)}
	return jsonify(response), 400

@app.errorhandler(404)
def page_not_found(e):
	response = {'code': 404, 'message': str(e)}
	return jsonify(response), 404

@app.errorhandler(500)
def internal_server_error(e):
	response = {'code': 500, 'message': str(e)}
	return jsonify(response), 500

@app.errorhandler(Exception)
def handle_exception(e):
	# pass through HTTP errors
	if isinstance(e, HTTPException):
		return e

	response = {"code": 500, "message": str(e)}
	return jsonify(response), 500

app.register_error_handler(400, handler_bad_request)
app.register_error_handler(404, page_not_found)
app.register_error_handler(500, internal_server_error)
app.register_error_handler(Exception, handle_exception)


# global variable
g_nslock = threading.Lock()
g_namespaces = {}
g_fragid = 0
g_fragcnt = 0
g_index_uri = None
g_db_uri = None
g_db_storage_options = None
g_index_config = None
g_role = None
g_redis_host = None
g_user = None
g_schema = None

def set_index(idx, namespace):
	ns = g_namespaces.get(namespace)
	if ns is not None:
		raise Exception('namespace {} already exist'.format(namespace))
	g_namespaces[namespace] = idx

def get_index(namespace='default'):
	return g_namespaces.get(namespace)

def load_all_namespaces():

	nss = Index.get_all_namespaces(g_db_storage_options, g_db_uri, g_index_config.name)

	# load all namespaces index here
	with g_nslock:
		for ns in nss:
			p = Index(config=g_index_config, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
				storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace=ns)
			p.load()
			set_index(p, ns)

def create_all_namespaces():
	pass

def global_init():

	global g_namespaces, g_fragid, g_fragcnt, g_index_uri, g_db_uri, g_db_storage_options, g_index_config, g_role, g_redis_host, g_user
	global g_schema

	g_fragid = int(os.environ.get('CLOUD_RUN_TASK_INDEX'))
	g_fragcnt = int(os.environ.get('CLOUD_RUN_TASK_COUNT'))

	g_index_uri = os.environ.get('INDEX_URI')
	if g_index_uri is None:
		raise ValueError('INDEX_URI env not found')

	g_db_uri = os.environ.get('DATABASE_URI')
	if g_db_uri is None:
		raise ValueError('DATABASE_URI env not found')

	g_db_storage_options = None

	if g_index_uri.startswith("s3://") or g_index_uri.startswith("s3a://"):
		aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
		if aws_id is None:
			raise ValueError('AWS_ACCESS_KEY_ID env not found')

		aws_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
		if aws_key is None:
			raise ValueError('AWS_SECRET_ACCESS_KEY env not found')

		aws_region = os.environ.get('AWS_REGION')
		if aws_region is None:
			raise ValueError('AWS_REGION env not found')

		aws_lock_provider = os.environ.get('AWS_S3_LOCKING_PROVIDER')
		if aws_lock_provider is None:
			raise ValueError('AWS_S3_LOCKING_PROVIDER env not found')

		aws_lock_table = os.environ.get('DELTA_DYNAMO_TABLE_NAME')
		if aws_lock_table is None:
			raise ValueError('DELTA_DYNAMO_TABLE_NAME env not found')

		g_db_storage_options = {'AWS_ACCESS_KEY_ID': aws_id,
								'AWS_SECRET_ACCESS_KEY': aws_key,
								'AWS_REGION': aws_region,
								'AWS_S3_LOCKING_PROVIDER': aws_lock_provider,
								'DELTA_DYNAMO_TABLE_NAME': aws_lock_table}
	elif g_index_uri.startswith("gs://"):
		gs_secret = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
		if gs_secret is None:
			raise ValueError('GOOGLE_APPLICATION_CREDENTIALS_JSON env not found')

		fpath = os.path.join(os.environ.get('HOME'), '.google.json')
		if not os.path.exists(fpath):
			with open(fpath, 'w') as f:
				f.write(gs_secret)

		os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = fpath
		g_db_storage_options = {'GOOGLE_SERVICE_ACCOUNT': fpath,
								'GOOGLE_APPLICATION_CREDENTIALS': fpath}

	# should be initialize here
	g_role = os.environ.get('KV_ROLE')
	if g_role is None:
		raise ValueError('KV_ROLE env not found')

	g_redis_host = os.environ.get('REDIS_HOST')
	if g_redis_host is None:
		raise ValueError('REDIS_HOST env not found')

	g_user = os.environ.get('API_USER')
	if g_user is None:
		raise ValueError('API_USER env not found')

	g_index_config = IndexConfig.from_json(os.environ.get('INDEX_JSON'))
	if g_index_config is None:
		raise ValueError('INDEX_JSON env not found')

	# global init index. If there is a index file indexdir/$fragid.hnsw found, load the index file into the memory
	if g_role == 'singleton' or g_role == 'query-segment':
		load_all_namespaces()
	#elif g_role == 'index-segment':
	#	create_all_namespaces()


@app.route("/create", methods=['POST'])
def create_index():
	if not request.is_json:
		abort(400, 'request is not in JSON format')

	data = request.json
	ns = 'default'
	try:
		if g_role == 'singleton':
			with g_nslock:
				idx = Index(config=g_index_config, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
					storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace=ns)
				idx.create()
				set_index(idx, ns)

		elif g_role == 'index-master':
			# create index-segment and invoke /create on index-segment
			pass
		elif g_role == 'index-segment':
			with g_nslock:
				idx = Index(config=g_index_config, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
					storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace=ns)
				idx.create()
				set_index(idx, ns)


	except Exception as e:
		abort(500, description = str(e))

	response = {'code': 200, 'message': 'ok'}
	return jsonify(response)

@app.route("/remove", methods=['GET'])
def remove_index():
	with g_nslock:
		for ns, idx in g_namespaces.items():
			idx.delete()
		g_namespaces.clear()

	response = {'code': 200, 'message': 'ok'}
	return jsonify(response)

@app.route("/insert", methods=['POST'])
def insert():
	if not request.is_json:
		abort(400, 'request is not in JSON format')
	
	data = request.json

	ns = data.get('namespace')
	if ns is None:
		ns = 'default'

	idx = None
	with g_nslock:
		idx = get_index(ns)
		if idx is None:
			# create a new namespace
			idx = Index(config=g_index_config, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
				storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace=ns)
			idx.create()
			set_index(idx, ns)
	
	# get index and do the insert here
	idx.insertData(data)
	response = {'code': 200, 'message': 'ok'}
	return jsonify(response)



@app.route("/update", methods=['POST'])
def update():
	pass

@app.route("/delete", methods=['POST'])
def delete():
	pass

@app.route("/status", methods=['GET'])
def status():
	status = {}
	for ns, idx in g_namespaces.items():
		s = idx.status()
		status[ns] = s

	return jsonify(status)
		
@app.route("/query", methods=['POST'])
def query():
	if not request.is_json:
		abort(400, 'request is not in JSON format')

	req = request.json	
	if req.get('vector') is None:
		raise ValueError('vector not found')

	output_fields = req.get('output_fields')
	if output_fields is None:
		raise ValueError('output_fields not found')

	req_filters = req.get('filter')

	vector = np.float32([req['vector']])
	ns = req.get('namespace')
	if ns is None:
		ns = 'default'

	idx = get_index(ns)
	if idx is None:
		raise ValueError('namespace not found')

	ids, distances = idx.query(req)

	print(ids[0])
	print(distances[0])

	if g_role != 'singleton':
		response = {'ids': ids[0], 'distances': distances[0]}
		return jsonify(response)

	schema = g_index_config.schema

	pri = schema.get_primary()

	columns = output_fields.copy()
	if pri.name not in columns:
		columns.append(pri.name)

	filters = [(pri.name, 'in', ids[0])]
	if req_filters is not None:
		for f in req_filters:
			filters.append(tuple(f))
	
	print(columns)
	print(filters)
	df = idx.filter(columns, filters=filters)

	results = []
	for index, row in df.iterrows():
		id = row[pri.name]
		idx = np.where(ids[0] == id)[0][0]
		distance = distances[0][idx]
		t = [distance.item(), id]
		for f in output_fields:
			t.append(row[f])
		results.append(tuple(t))

	results = sorted(results, key=itemgetter(0))
	print(results)

	res = {}
	res['distances'] = []
	res['ids'] = []

	fields = {}
	for f in output_fields:
		fields[f] = []

	for r in results:
		res['distances'].append(r[0])
		res['ids'].append(r[1])
		i=2
		for f in output_fields:
			fields[f].append(r[i])
			i+=1
	res['output_fields'] = fields

	response = {'code': 200, 'data': res}
	print(response)
	return jsonify(response)


@app.route('/flush', methods=['GET'])
def flush():
	with g_nslock:
		for key, idx in g_namespaces.items():
			print('flush key={}'.format(key))
			idx.flush()
	response = {'code': 200, 'message': 'ok'}
	return jsonify(response)
			

def run(debug=False):
	app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=debug)


# run global init here
global_init()
