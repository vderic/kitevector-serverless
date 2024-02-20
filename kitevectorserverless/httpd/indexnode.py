import json
import os
import threading
from werkzeug.exceptions import HTTPException, BadRequest
from flask import Flask, request, json, abort, jsonify
from kitevectorserverless.index import Index

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

	response = {"code": e.code, "message": str(e)}
	return jsonify(response), e.code

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

def set_index(idx, namespace):
	ns = g_namespaces.get(namespace)
	if ns is not None:
		raise Exception('namespace {} already exist'.format(namespace))
	g_namespaces[namespace] = idx

def get_index(namespace='default'):
	return g_namespaces.get(namespace)

def load_all_namespaces():

	# TODO: get list of namespaces here from Redis LRANGE, key = namespace:$user:$indexname, value = [ns1,ns2]
	list_ns = ['default']

	# load all namespaces index here
	with g_nslock:
		for ns in list_ns:
			p = Index(config=g_index_config, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
				storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace=ns)
			p.create()
			set_index(p, ns)

def create_all_namespaces():
	pass

def global_init():

	global g_namespaces, g_fragid, g_fragcnt, g_index_uri, g_db_uri, g_db_storage_options, g_index_config, g_role, g_redis_host, g_user

	g_fragid = int(os.environ.get('CLOUD_RUN_TASK_INDEX'))
	g_fragcnt = int(os.environ.get('CLOUD_RUN_TASK_COUNT'))

	g_index_uri = os.environ.get('INDEX_URI')
	g_db_uri = os.environ.get('DATABASE_URI')
	g_db_storage_options = None

	if g_index_uri.startswith("s3://") or g_index_uri.startswith("s3a://"):
		aws_id = os.environ['AWS_ACCESS_KEY_ID']
		aws_key = os.environ['AWS_SECRET_ACCESS_KEY']
		aws_region = os.environ['AWS_REGION']
		aws_lock_provider = os.environ['AWS_S3_LOCKING_PROVIDER']
		aws_lock_table = os.environ['DELTA_DYNAMO_TABLE_NAME']
		g_db_storage_options = {'AWS_ACCESS_KEY_ID': aws_id,
								'AWS_SECRET_ACCESS_KEY': aws_key,
								'AWS_REGION': aws_region,
								'AWS_S3_LOCKING_PROVIDER': aws_lock_provider,
								'DELTA_DYNAMO_TABLE_NAME': aws_lock_table}
	elif g_index_uri.startswith("gs://"):
		gs_account = os.environ['GOOGLE_SERVICE_ACCOUNT']
		gs_key = os.environ['GOOGLE_SERVICE_ACCOUNT_KEY']
		g_db_storage_options = {'GOOGLE_SERVICE_ACCOUNT': gs_account,
								'GOOGLE_SERVICE_ACCOUNT_KEY': gs_key}

	# should be initialize here
	g_index_config = json.loads(os.environ.get('INDEX_JSON'))
	g_role = os.environ.get('KV_ROLE')
	g_redis_host = os.environ.get('REDIS_HOST')
	g_user = os.environ.get('API_USER')

	print(g_index_uri)
	print(g_db_uri)

	# global init index. If there is a index file indexdir/$fragid.hnsw found, load the index file into the memory
	#if g_role == 'singleton' or g_role == 'query-segment':
	#	load_all_namespaces()
	#elif g_role == 'index-segment':
	#	create_all_namespaces()


global_init()


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
	name = request.args.get('index')
	if name is None:
		abort(400, 'index is not found in request')

	with g_nslock:
		for ns, idx in g_namespaces.items():
			idx.delete()

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
		

def run():
	app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)

