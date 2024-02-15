import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print(r.get('idx:counter'))

print(r.incr('idx:counter'))
print(r.incrby('idx:counter', 100))
print(r.get('apple'))
