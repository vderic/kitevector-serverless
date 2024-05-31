import time
import sys
import heapq
import numpy as np
import pyarrow as pa
from kitevectorserverless.datatype import IndexConfig, Schema
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError, TableNotFoundError, CommitFailedError

class Expr:

    def __init__(self, expr):
        self.expr = expr
        
    def __str__(self):
        return self.expr

    def sql(self):
        return self.expr

    def tuple(self):
        return (self.expr)

class Var(Expr):
    def __init__(self, cname):
        self.cname = cname

    def __str__(self):
        return self.cname

    def sql(self):
        return self.cname

class VectorExpr(Expr):
    def __init__(self, cname):
        self.cname = cname

    def inner_product(self, embedding):
        return OpExpr('<#>', self, Embedding(embedding))

    def l2_distance(self, embedding):
        return OpExpr('<->', self, Embedding(embedding))

    def cosine_distance(self, embedding):
        return OpExpr('<=>', self, Embedding(embedding))

    def __str__(self):
        return self.cname

    def sql(self):
        return self.__str__()

class Embedding(Expr):
    
    def __init__(self, embedding):
        self.embedding = embedding
    
    def __str__(self):
        return '\'{' + ','.join([str(e) for e in self.embedding]) + '}\''

    def sql(self):
        return '\'[' + ','.join([str(e) for e in self.embedding]) + ']\''

class ScalarArrayOpExpr(Expr):

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        ret = '''{} IN ({})'''.format(self.left, ','.join([str(e) for e in self.right]))
        return ret

    def sql(self):
        return self.__str__()

    def tuple(self):
        return (self.left, 'in', self.right)

class OpExpr(Expr):

    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right

    def __str__(self):
        leftsql = None
        if isinstance(self.left, Expr):
            leftsql = str(self.left)
        elif isinstance(self.left, list) or isinstance(self.left, np.ndarray):
            leftsql = '\'{' + ','.join([str(e) for e in self.left]) + '}\''
        else:
            leftsql = str(self.left)

        rightsql = None
        if isinstance(self.right, Expr):
            rightsql = str(self.right)
        elif isinstance(self.right, list) or isinstance(self.right, np.ndarray):
            rightsql = '\'{' + ','.join([str(e) for e in self.right]) + '}\''
        else:
            rightsql = str(self.right)

        ret = '''{} {} {}'''.format(leftsql, self.op, rightsql)
        return ret

    def sql(self):
        leftsql = None
        if isinstance(self.left, Expr):
            leftsql = self.left.sql()
        elif isinstance(self.left, list) or isinstance(self.left, np.ndarray):
            leftsql = '\'{' + ','.join([e.sql() if isinstance(e, Expr) else str(e) for e in self.left]) + '}\''
        else:
            leftsql = str(self.left)

        rightsql = None
        if isinstance(self.right, Expr):
            rightsql = self.right.sql()
        elif isinstance(self.right, list) or isinstance(self.right, np.ndarray):
            rightsql = '\'{' + ','.join([e.sql() if isinstance(e, Expr) else str(e) for e in self.right]) + '}\''
        else:
            rightsql = str(self.right)

        ret = '''{} {} {}'''.format(leftsql, self.op, rightsql)
        return ret

    def tuple(self):
        return (self.left, self.op, self.right)

class BaseVector:

    def __init__(self):
        self.projection = None
        self.orderby = None
        self.filters = []
        self.nlimit = None
        self.path = None
        self.filespec = None
        self.index_params = None
        self.index_hosts = None
        self.data = None

    def select(self, projection):
        self.projection = projection
        return self

    def limit(self, limit):
        self.nlimit = limit
        return self

    def filter(self, expr):
        self.filters.append(expr)
        return self

    def table(self, path):
        self.path = path
        return self
    
    def format(self, filespec):
        self.filespec = filespec
        return self

class KVDeltaTable(BaseVector):

    def __init__(self, table_uri, schema, storage_options=None):
        super().__init__()
        self.table_uri = table_uri
        self.storage_options=storage_options
        self.schema = schema
        self.pa_schema = self.to_pyarrow_schema(schema)
        self.dt = None
        self.primary_cno = self.primary_column()
        if self.primary_cno == -1:
            raise ValueError('primary id column not found')

        self.vector_cno = self.vector_column()
        if self.vector_cno == -1:
            raise ValueError('vector column not found')

        print(self.primary_cno)
        print(self.vector_cno)

    def get_column_name(self, cno):
        return self.pa_schema.names[cno]

    def get_column_type(self, cno):
        return self.pa_schema.types[cno]

    def primary_column(self):
        fields = self.schema.fields
        idx = 0
        for f in fields:
            if f.is_primary:
                return idx
            idx += 1
        return -1

    def vector_column(self):
        fields = self.schema.fields
        idx = 0
        for f in fields:
            if f.type == 'vector':
                return idx
            idx += 1
        return -1

    def get_ids_vectors(self, data):
        primary_cname = self.pa_schema.names[self.primary_cno]
        primary_type = self.pa_schema.types[self.primary_cno]
        vector_cname = self.pa_schema.names[self.vector_cno]
        vector_type = self.pa_schema.types[self.vector_cno]

        #return pa.array(data[primary_cname], primary_type).to_numpy(), pa.array(data[vector_cname], vector_type).to_numpy()
        return data[primary_cname], np.float32(data[vector_cname])

    def to_pyarrow_schema(self, schema):
        pafields = []

        fields = schema.fields
        for f in fields:
            cname = f.name
            dtype = None
            if f.type == 'int8':
                dtype = pa.int8()
            elif f.type == 'int16':
                dtype = pa.int16()
            elif f.type == 'int32':
                dtype = pa.int32()
            elif f.type == 'int64':
                dtype = pa.int64()
            elif f.type == 'float':
                dtype = pa.float32()
            elif f.type == 'double':
                dtype = pa.float64()
            elif f.type == 'string':
                dtype = pa.string()
            elif f.type == 'vector':
                dtype = pa.list_(pa.float32())
            else:
                raise ValueError('unsupport data type {}'.format(f.type))

            pafields.append(pa.field(cname, dtype))

        return pa.schema(pafields)
        

    def to_pyarrow_table(self, data_json):
        names = self.pa_schema.names
        types = self.pa_schema.types

        padata = []
        for name, dtype in zip(names, types):
            padata.append(pa.array(data_json[name], dtype))
            
        return pa.Table.from_arrays(padata, names = names)


    def get_dt(self):
        if self.dt is None:
            self.dt = DeltaTable(self.table_uri, storage_options=self.storage_options)
        return self.dt

    def create(self):
        self.dt = DeltaTable.create(self.table_uri, schema=self.pa_schema, storage_options=self.storage_options, mode='error', configuration={"delta.enableChangeDataFeed": "true"})
        #self.dt = DeltaTable.create(self.table_uri, schema=self.schema, storage_options=self.storage_options, mode='overwrite')

    def to_pandas(self, columns=None, filters=None):
        dt = self.get_dt()
        return dt.to_pandas(columns=columns, filters=filters)

    def insert(self, data):
        tab = self.to_pyarrow_table(data)
        dt = self.get_dt()
        write_deltalake(dt, tab, mode='append')

    def upsert(self, data):
        tab = self.to_pyarrow_table(data)
        dt = self.get_dt()

        dt.merge(source=tab,
                predicate="target.id = source.id",
                source_alias="source",
                target_alias="target").when_matched_update_all().when_not_matched_insert_all().execute()
                

    def delete(self, predicate=None):
        dt = self.get_dt()
        dt.delete(predicate)

    def execute(self):

        if self.projection is None:
            raise ValueError('No projection found')

        tuples = None
        if self.filters is not None and len(self.filters) > 0:
            tuples = [f.tuple() for f in self.filters]

        dt = self.get_dt()
        df = dt.to_pandas(columns=self.projection, filters=tuples)
        return df


if __name__ == '__main__':
    try:

        index_dict = {'schema': {'fields': [ {'name': 'id', 'is_primary': True, 'type': 'int64'},
                                    {'name':'vector', 'type': 'vector'},
                                    {'name':'animal', 'type': 'string'}]
                                },
                        'dimension': 1536,
                        'metric_type': 'ip',
                        }

        data = {'id':[1,2,3,4], 
                'vector':[[1.3,2.3,4.5,3.4], [1.3,4.5,6.3,2.6], [4.3,6.3,2.1,4.2], [2.6, 4.5,7.5,3.2]], 
                'animal':['tiger', 'fox', 'frog', 'cat']}

        newdata = {'id':[1,2,5,6], 
                'vector':[[1.2,1.3,4.5,3.4], [1.1,4.3,6.3,2.6], [4.3,6.3,2.1,4.2], [2.6, 4.5,7.5,3.2]], 
                'animal':['fruite', 'apple', 'frog', 'cat']}

        dt = KVDeltaTable("kvdb", index_dict['schema'])
        dt.create()
        dt.insert(data)

        time.sleep(0.1)

        df = dt.select(['vector', 'id']).filter(OpExpr('=', 'animal', 'fox')).filter(ScalarArrayOpExpr('id', [2,3])).execute()
        #df = dt.to_pandas(columns=['vector'], filters=[('id', 'in', [2,3]), ('animal', '=', 'apple')])

        print(df.to_string())
        time.sleep(0.1)
    except DeltaError as e:
        print(e)

