import time
import sys
import heapq
import numpy as np
import pyarrow as pa
from kitevectorserverless.datatype import IndexConfig, Schema
from kitevectorserverless.db import KVDeltaTable, OpExpr, ScalarArrayOpExpr
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError, TableNotFoundError, CommitFailedError



if __name__ == '__main__':
    try:

        JSON={"name":"serverless",
            "dimension" : 1536,
            "metric_type" : "ip",
            "schema": { "fields" : [{"name": "id", "type":"int64", "is_primary": "true"},
                {"name":"vector", "type":"vector"},
                {"name":"animal", "type":"string"}
             ]},
            "params": {"max_elements" : 1000, "ef_construction":48, "M": 24}
         }

        index_dict = {'schema': {'fields': [ {'name': 'id', 'is_primary': True, 'type': 'int64'},
                                    {'name':'vector', 'type': 'vector'},
                                    {'name':'animal', 'type': 'string'}]
                                },
                        'dimension': 1536,
                        'metric_type': 'ip',
                        'name':'testing'
                        }

        config = IndexConfig.from_dict(JSON)

        data = {'id':[1,2,3,4],
                'vector':[[1.3,2.3,4.5,3.4], [1.3,4.5,6.3,2.6], [4.3,6.3,2.1,4.2], [2.6, 4.5,7.5,3.2]],
                'animal':['tiger', 'fox', 'frog', 'cat']}

        newdata = {'id':[1,2,5,6],
                'vector':[[1.2,1.3,4.5,3.4], [1.1,4.3,6.3,2.6], [4.3,6.3,2.1,4.2], [2.6, 4.5,7.5,3.2]],
                'animal':['fruite', 'apple', 'frog', 'cat']}

        dt = KVDeltaTable("kvdb", config.schema)

        created = True
        if created == False:
            dt.create()
        dt.upsert(data)
        dt.upsert(newdata)

        time.sleep(0.1)

        df = dt.select(['vector', 'id', 'animal']).execute()
        #df = dt.select(['vector', 'id']).filter(OpExpr('=', 'animal', 'fox')).filter(ScalarArrayOpExpr('id', [2,3])).execute()
        #df = dt.to_pandas(columns=['vector'], filters=[('id', 'in', [2,3]), ('animal', '=', 'apple')])

        print(df.to_string())
        time.sleep(0.1)

        files = dt.get_dt().file_uris()
        print(files)
    except DeltaError as e:
        print(e)

