import tempfile
import shutil
import os
import json
import numpy as np
import pickle
import hnswlib
from functools import partial
import argparse
import threading
import glob
import heapq
from readerwriterlock import rwlock
import redis
from kitevectorserverless import db
from kitevectorserverless.storage import FileStorageFactory

class IndexSort:

	def __init__(self, nbest):
		self.heap = []
		self.nbest = nbest

	def add(self, ids, distances):
		for id, score in zip(ids, distances):
			# NOTE: inner product need negative
			score *= -1
			if len(self.heap) <= self.nbest:
				heapq.heappush(self.heap, (score, id))
			else:
				heapq.heapreplace(self.heap, (score, id))
		
	def get(self):
		if len(self.heap) == self.nbest+1:
			heapq.heappop(self.heap)

		scores = []
		ids = []
		for i in range(len(self.heap)):
			t = heapq.heappop(self.heap)
			scores.insert(0, t[0])
			ids.insert(0, t[1])

		return ids, scores


# GCP env variables for cloud jobs CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT
# REDIS_HOST
# DATABASE_ENDPOINT
class Index:

	def __init__(self, config, fragid, index_uri, db_uri, storage_options, redis, role, user, namespace='default'):
		self.config = config
		self.fragid = fragid
		self.datadir = os.path.join(index_uri, config['name'], namespace)
		self.db_uri = os.path.join(db_uri, config['name'], namespace)
		self.db_storage_options = storage_options
		self.redis_host = redis
		self.role = role
		self.user = user
		self.lock = rwlock.RWLockFair()
		self.index = None
		self.namespace = namespace
		self.fs = FileStorageFactory.create(storage_options)

		#self.load(datadir)

	@staticmethod
	def get_all_namespaces(storage_options, db_uri, index_name):
		fs = FileStorageFactory.create(storage_options)
		prefix = os.path.join(db_uri, index_name + '/')
		dirs = fs.listdir(prefix)
		nss = [ d.removeprefix(prefix).removesuffix('/') for d in dirs]
		return nss


	def get_redis(self):
		r = redis.Redis(host= self.redis_host)
		return r

	def load(self):
		with self.lock.gen_wlock():
			# load delta table
			db_table = db.KVDeltaTable(self.db_uri, self.config['schema'], self.db_storage_options)
			db_table.get_dt()

			# load index file
			fpath = os.path.join(self.datadir, '{}.hnsw'.format(str(self.fragid)))
			print(fpath)
			if not self.fs.exists(fpath):
				# create a new index
				self.create_index()
			else:
				with tempfile.NamedTemporaryFile(delete=False) as fp:
					try:
						fp.close()
						self.fs.download(fpath, fp.name)
						space = self.config['metric_type']
						dim = self.config['dimension']
						p = hnswlib.Index(space=space, dim = dim)
						p.load_index(fp.name)
						self.index = p
						print(f"load index {fpath}")
					finally:
						os.unlink(fp.name)
							


				
	def query(self, req):	
		with self.lock.gen_rlock():
			# found the index and get the nbest
			embedding = np.float32([req['vector']])
			params = req['params']['params']
			ef = params['ef']
			k  = params['k']
			num_threads = 1
			self.index.set_ef(ef)
			self.index.set_num_threads(num_threads)
			ids, distances = self.index.knn_query(embedding, k=k)
			return ids, distances

	def save_index_meta(self, cache, req):
		user = os.environ.get('API_USER')
		key = 'index:{}:{}'.format(user, req['name'])
		value = json.dumps(req)
		cache.set(key, value)

	def get_index_meta(self, cache, idxname):
		user = os.environ.get('API_USER')
		key = 'index:{}:{}'.format(user, idxname)
		jsonstr = cache.get(key)
		if jsonstr is None:
			return None
		return json.loads(jsonstr)

	def delete_index_meta(self, cache, idxname):
		user = os.environ.get('API_USER')
		key = 'index:{}:{}'.format(user, idxname)
		return cache.delete(key)
		
	
	def create_index(self):
		# create index inside the lock
		space = self.config['metric_type']
		dim = self.config['dimension']
		params = self.config['params']
		max_elements = params['max_elements']
		ef_construction = params['ef_construction']
		M = params['M']
		#num_threads = params['num_threads']
		p = hnswlib.Index(space=space, dim = dim)
		p.init_index(max_elements=max_elements, ef_construction=ef_construction, M=M)
		#p.set_num_threads(num_threads)

		# save index to processing index so that we can keep track of the status
		self.index = p
	
	def create_delta_table(self):
		if self.role == 'singleton' or self.role == 'index-master':
			db_table = db.KVDeltaTable(self.db_uri, self.config['schema'], self.db_storage_options)
			db_table.create()
		elif self.role == 'index-segment':
			# check the db_uri exists
			db_table = db.KVDeltaTable(self.db_uri, self.config['schema'], self.db_storage_options)
			db_table.get_dt()


	def create(self):
		with self.lock.gen_wlock():
			self.create_index()
			self.create_delta_table()

	def insertData(self, req):
		# TODO: get namespace from request
		table = db.KVDeltaTable(self.db_uri, self.config['schema'], self.db_storage_options)
		ids, vectors = table.get_ids_vectors(req)
		with self.lock.gen_wlock():
			# TODO: check index full
			if self.index.element_count + len(ids) > self.index.max_elements:
				raise Exception('index is full')
			self.index.add_items(vectors, ids)
			table.insert(req)

	def updateData(self, req):
		with self.lock.gen_wlock():
			pass

	def deleteData(self, req):
		with self.lock.gen_wlock():
			pass

	def delete(self):
		with self.lock.gen_wlock():
			fpath = self.get_index_filename()
			if self.fs.exists(fpath):
				self.fs.remove(fpath)

			self.fs.rmtree(self.db_uri)

	def status(self):
		if self.index is None:
			return {'status':'error', 'message': 'index not found'}

		return {'status':'ok', 'element_count': self.index.element_count, 'max_elements': self.index.max_elements}

	def get_index_filename(self):
		return os.path.join(self.datadir, '{}.hnsw'.format(self.fragid))

	def flush(self):
		with self.lock.gen_wlock():
			fpath = self.get_index_filename()
			if self.index.element_count <= 0:
				return
			with tempfile.NamedTemporaryFile(delete=False) as fp:
				try:
					fp.close()
					self.index.save_index(fp.name)
					self.fs.upload(fp.name, fpath)
					print(f"index file uploaded to {fpath}")
				finally:
					os.unlink(fp.name)

