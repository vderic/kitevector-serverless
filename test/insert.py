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
            {"name":"vector", "type":"vector"},
            {"name":"animal", "type":"string"}
            ]},
        "params": {"max_elements" : 1000, "ef_construction":48, "M": 24}
        }

    N=100
    dim = 1536

    vectors = [ gen_embedding(dim) for i in range(N)]

    data = {'id': [ random.randint(1, 10000) for i in range(N)],
            'vector': vectors,
            'animal': [ 'str' + str(n) for n in range(N)]}

    jsonstr = json.dumps(data)

    conn = http.client.HTTPConnection('localhost', 8080)
    headers = {'Content-Type': 'application/json'}

    
    conn.request('POST', '/upsert', jsonstr, headers)
    response = conn.getresponse()
    print(response.status, response.reason)
    data = response.read()
    print(data)
    conn.close()


