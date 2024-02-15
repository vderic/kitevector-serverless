from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError, TableNotFoundError, CommitFailedError
import pyarrow as pa
import sys

if __name__ == "__main__":
	
	try:
		table_uri = "kvdb"
		ids = pa.array([1,2,3,4], pa.int64())
		vectors = pa.array([[1.0,3,4,2],[1.0,3,4,5], [3.0,5,6,7], [4.0,5,68,6]], pa.list_(pa.float32()))
		names = ["id", "vector"]
		data = pa.Table.from_arrays([ids, vectors], names = names)
		for i in range(1000):
			write_deltalake(table_uri, data, mode='append')
			dt = DeltaTable(table_uri)
			print(dt.to_pandas())
			#pd = dt.to_pandas()
			#print(pd)

	except DeltaError as e:
		print(e)
