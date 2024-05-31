import polars
from deltalake import DeltaTable

dt = DeltaTable("kvdb")
table = dt.load_cdf(starting_version=0, ending_version=4).read_all()
pt = polars.from_arrow(table)
print(pt)
#pt.group_by("_commit_version").len().sort("len", descending=True)

df = dt.get_add_actions(True).to_pandas()
print(df)

h = dt.history()

version = dt.version()


print(dt.metadata())

#r = dt.optimize.z_order(["id"])
#print(r)

print(version)
dt.vacuum()

files = dt.file_uris()
print(files)

