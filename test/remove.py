import http.client

if __name__ == '__main__':
	
	conn = http.client.HTTPConnection('localhost', 8080)
	headers = {'Content-Type': 'application/json'}

	conn.request('GET', '/remove')
	response = conn.getresponse()
	print(response.status, response.reason)
	data = response.read()
	print(data)
	conn.close()


