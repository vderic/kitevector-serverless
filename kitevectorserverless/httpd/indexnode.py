import os
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
	if instance(e, HTTPException):
		return e

	response = e.get_response()
	response.data = json.dumps({
		"code": e.code,
		"name": e.name,
		"message": e.description,
	})
	response.content_type = "application/json"
	return response, e.code

app.register_error_handler(400, handler_bad_request)
app.register_error_handler(404, page_not_found)
app.register_error_handler(500, internal_server_error)
app.register_error_handler(Exception, handle_exception)


# global variable
g_namespaces = {}
g_fragid = 0
g_fragcnt = 0
g_index_uri = None
g_db_uri = None
g_db_storage_options = None
g_index_name = None
g_role = None
g_redis_host = None
g_user = None

def global_init():

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
	g_index_name = os.environ.get('INDEX_NAME')
	g_role = os.environ.get('KV_ROLE')
	g_redis_host = os.environ.get('REDIS_HOST')
	g_user = os.environ.get('API_USER')

	print(g_index_uri)
	print(g_db_uri)

	# global init index. If there is a index file indexdir/$fragid.hnsw found, load the index file into the memory
	# load all namespaces index here
	g_namespaces['default'] = Index(name=g_index_name, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
		storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace='default')


global_init()


def get_namespace(namespace='default'):
	ns = g_namespaces.get(namespace)
	if ns is not None:
		return ns
	
	g_namespaces[namespace] = Index(name=g_index_name, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
		storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace=namespace)

	return g_namespaces[namespace]


@app.route("/create", methods=['POST'])
def create_index():
	if request.is_json:
		data = request.json
		idx = get_namespace(namespace='default')
		try:
			idx.create(data)
		except Exception as e:
			abort(500, description = str(e))
	else:
		abort(400, 'request is not in JSON format')
	return data

@app.route("/remove", methods=['GET'])
def delete_index():
	name = request.args.get('index')
	if name is None:
		abort(400, 'index is not found in request')

	for ns, idx in g_namespaces.items():
		idx.delete()

	response = {'code': 200, 'message': 'ok'}
	return jsonify(response)

@app.route("/insert")
def insert():
	pass

@app.route("/update")
def update():
	pass

@app.route("/delete")
def delete():
	pass

@app.route("/status")
def status():
	pass

def run():
	app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)

