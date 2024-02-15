import time
import sys
import heapq
import numpy as np
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError, TableNotFoundError, CommitFailedError
from kitevectorserverless import index,db


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

		dt = db.KVDeltaTable("kvdb", index_dict['schema'])
		#dt.create()
		dt.insert(data)

		time.sleep(0.1)

		df = dt.select(['vector', 'id']).filter(db.OpExpr('=', 'animal', 'fox')).filter(db.ScalarArrayOpExpr('id', [2,3])).execute()
		#df = dt.to_pandas(columns=['vector'], filters=[('id', 'in', [2,3]), ('animal', '=', 'apple')])

		print(df.to_string())
		time.sleep(0.1)
	except DeltaError as e:
		print(e)
