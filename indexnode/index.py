import os
import json
import numpy as np
import pickle
import hnswlib
from kite import kite
from kite.xrg import xrg
from functools import partial
import argparse
import threading
import glob
import heapq
from readerwriterlock import rwlock
import redis

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

    datadir = None
    indexes = {}
    idxlocks = {}
    fragid = 0

    @classmethod
    def init(cls, fragid, datadir):
        cls.datadir = datadir
        cls.fragid = fragid

        #cls.load(datadir)

    @classmethod
    def get_indexkey(cls, idxname, namespace=''):
        return '{}#{}#{}'.format(idxname, namespace, cls.fragid)

    @classmethod
    def index_exists(cls, req):
        key = cls.get_indexkey(req['name'])
        return cls.indexes.get(key) is not None

    @classmethod
    def get_lock(cls, idxname):
        lock = cls.idxlocks.get(idxname)
        if lock == None:
            lock = rwlock.RWLockFair()
            cls.idxlocks[idxname] = lock
        return lock

    @classmethod
    def get_redis(cls):
        r = redis.Redis(host=os.environ.get('REDIS_HOST', 'localhost'))
        return r

    @classmethod
    def load(cls, datadir):
        if not os.path.isdir(datadir):
            raise Exception("data directory not exists")
        
        flist = glob.glob('*.hnsw', root_dir = cls.datadir)
        for f in flist:
            idxname = os.path.splitext(os.path.basename(f))[0]
            fpath = os.path.join(cls.datadir, f)
            with cls.get_lock(idxname):
                # load the index inside the lock
                with open(fpath, 'rb') as fp:
                    idx = pickle.load(fp)
                    cls.indexes[idxname] = idx

    @classmethod
    def query(cls, req):    
        idx = None
        idxkey = cls.get_indexkey(req['name'])
        with cls.get_lock(idxkey).gen_rlock():
            idx = cls.indexes[idxkey]

            # found the index and get the nbest
            embedding = np.float32(req['embedding'])
            params = req['index_params']['params']
            ef = params['ef']
            k  = params['k']
            num_threads = params['num_threads']
            idx.set_ef(ef)
            idx.set_num_threads(num_threads)
            ids, distances = idx.knn_query(embedding, k=k)
            return ids, distances

    @classmethod
    def save_index_meta(cls, cache, req):
        user = os.environ.get('API_USER')
        key = 'index:{}:{}'.format(user, req['name'])
        value = json.dumps(req)
        cache.set(key, value)

    @classmethod
    def get_index_meta(cls, cache, idxname):
        user = os.environ.get('API_USER')
        key = 'index:{}:{}'.format(user, idxname)
        return cache.get(key)

    @classmethod
    def delete_index_meta(cls, cache, idxname):
        user = os.environ.get('API_USER')
        key = 'index:{}:{}'.format(user, idxname)
        return cache.delete(key)
        
    @classmethod
    def create(cls, req):
        idxkey = cls.get_indexkey(req['name'])
        with cls.get_lock(idxkey).gen_wlock():
            # create index inside the lock
            space = req['metric_type']
            dim = req['dimension']
            params = req['params']
            max_elements = params['max_elements']
            ef_construction = params['ef_construction']
            M = params['M']
            #num_threads = params['num_threads']
            p = hnswlib.Index(space=space, dim = dim)
            p.init_index(max_elements=max_elements, ef_construction=ef_construction, M=M)
            #p.set_num_threads(num_threads)

            # TODO: save the index metadata to database and redis
            r = cls.get_redis()
            idxcfg = cls.get_index_meta(r, req['name'])
            if idxcfg is not None:
                raise ValueError('Index {} already exists'.format(req['name']))

            cls.save_index_meta(r, req)

            # save index to processing index so that we can keep track of the status
            cls.indexes[idxkey] = p

    @classmethod
    def insertData(cls, req):
        # TODO: get namespace from request
        idxkey = cls.get_indexkey(req['name'])
        with cls.get_lock(idxkey).gen_wlock():
            pass


    @classmethod
    def updateData(cls, req):
        idxkey = cls.get_indexkey(req['name'])
        with cls.get_lock(idxkey).gen_wlock():
            pass

    @classmethod
    def deleteData(cls, req):
        idxkey = cls.get_indexkey(req['name'])
        with cls.get_lock(idxkey).gen_wlock():
            pass

    @classmethod
    def delete(cls, idxname):
        idxkey = cls.get_indexkey(idxname)
        with cls.get_lock(idxkey).gen_wlock():
            fpath = os.path.join(cls.datadir, '{}.hnsw'.format(idxkey))
            if os.path.exists(fpath):
                os.remove(fpath)

            r = cls.get_redis()
            cls.delete_index_meta(r, idxname)
            
            cls.indexes.pop(idxkey)
            cls.idxlocks.pop(idxkey)

    @classmethod
    def status(cls, req):
        idxkey = cls.get_indexkey(req['name'])
        p = cls.indexes.get(idxkey)
        if p is None:
            p = cls.indexes.get(idxkey)
            if p is None:
                return {'status':'error', 'name': idxkey, 'message': 'index not found'}

            return {'status':'ok', 'name': idxkey, 'element_count': p.element_count, 'max_elements': p.max_elements}
        
        return {'status': 'processing', 'name': idxkey, 'element_count': p.element_count, 'max_elements': p.max_elements}

if __name__ == "__main__":

    os.environ['API_USER'] = 'vitesse'
    os.environ['REDIS_HOST'] = 'localhost'
    os.environ['DATABASE_HOME'] = 'deltalake'
    os.environ['INDEX_HOME'] = 'index'

    #CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT
    os.environ['CLOUD_RUN_TASK_INDEX'] = '0'
    os.environ['CLOUD_RUN_TASK_COUNT'] = '1'

    fragid = int(os.environ.get('CLOUD_RUN_TASK_INDEX'))
    fragcnt = int(os.environ.get('CLOUD_RUN_TASK_COUNT'))

    req = { 'name': 'movie', 
            'metric_type' : 'ip',
            'dimension' : 4,
            'params' : { 'max_elements' : 1000, 'ef_construction' : 48, 'M' : 24}
        }

    indexdir = os.environ.get('INDEX_HOME')
    datadir = os.environ.get('DATABASE_HOME')
    
    # global init here
    Index.init(fragid, indexdir)
    
    # REST API
    Index.create(req)
    
    Index.delete(req['name'])
