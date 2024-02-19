import os
from flask import Flask
from kitevectorserverless.index import Index

app = Flask(__name__)
g_indexes = {}
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

	# should be initialize here
	g_index_name = os.environ.get('INDEX_NAME')
	g_db_storage_options = None
	g_role = os.environ.get('KV_ROLE')
	g_redis_host = os.environ.get('REDIS_HOST')
	g_user = os.environ.get('API_USER')

	print(g_index_uri)
	print(g_db_uri)

	# global init index. If there is a index file indexdir/$fragid.hnsw found, load the index file into the memory
	# load all namespaces index here
	g_indexes['default'] = Index(name=g_index_name, fragid=g_fragid, index_uri=g_index_uri, db_uri=g_db_uri,
		storage_options=g_db_storage_options, redis=g_redis_host, role=g_role, user=g_user, namespace='default')

global_init()

@app.route("/create")
def create():
	pass

@app.route("/remove")
def remove():
	pass

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

