import http.client
import random
import math
import json
import sys
from datetime import datetime


def gen_embedding(nitem):
	ret = []
	for x in range(nitem):
		ret.append(random.uniform(-1,1))
	sum = 0
	for x in ret:
		sum += x*x
	sum = math.sqrt(sum)
	# normalize
	for i in range(len(ret)):
		ret[i] = ret[i] / sum
	return ret


if __name__ == '__main__':
	
	random.seed(datetime.now().timestamp())

	JSON={"name":"serverless",
		"dimension" : 1536,
		"metric_type" : "ip",
		"schema": { "fields" : [{"name": "id", "type":"int64", "is_primary": "true"},
			{"name":"vector", "type":"vector", 'is_anns': 'true'},
			{"name":"animal", "type":"string"}
			]},
		"params": {"max_elements" : 1000, "ef_construction":48, "M": 24}
		}

	N=100
	dim = 1536

	vector = gen_embedding(dim)
	search_params =  { 'offset': 0, 'params': {'ef': 25, 'k': 10} }


	req = {'vector': vector,
			'params': search_params,
			'output_fields': ['animal'],
			'limit': 10,
			'filter': [['animal', '=', 'fox']],
			'namespace': 'default'
			}

	req = {'vector': vector,
			'params': search_params,
			'output_fields': ['animal'],
			'limit': 10,
			'namespace': 'default'
			}

	jsonstr = json.dumps(req)

	conn = http.client.HTTPConnection('localhost', 8080)
	headers = {'Content-Type': 'application/json'}

	
	conn.request('POST', '/query', jsonstr, headers)
	response = conn.getresponse()
	print(response.status, response.reason)
	data = response.read()
	print(data)
	conn.close()


