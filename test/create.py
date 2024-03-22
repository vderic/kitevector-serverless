import http.client

if __name__ == '__main__':
    
    JSON='''{"name":"serverless",
        "dimension" : 1536,
        "metric_type" : "ip",
        "schema": { "fields" : [{"name": "id", "type":"int64", "is_primary": "true"},
            {"name":"vector", "type":"vector", "is_anns": "true"},
            {"name":"animal", "type":"string"}
            ]},
        "params": {"max_elements" : 1000, "ef_construction":48, "M": 24}
        }'''

    conn = http.client.HTTPConnection('localhost', 8080)
    headers = {'Content-Type': 'application/json'}

    conn.request('POST', '/create', JSON, headers)
    response = conn.getresponse()
    print(response.status, response.reason)
    data = response.read()
    print(data)
    conn.close()


