from redis import StrictRedis

r0 = StrictRedis(host="localhost", port=6379, decode_responses=True, db=0)

print(r0.set("name", "123"))
print(r0.get("name"))

print(r0.dbsize())