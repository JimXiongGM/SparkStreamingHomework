import time
from redis import StrictRedis
redis = StrictRedis(host="localhost", port=6379, decode_responses=True, db=1)

def test():
    while True:
        r = redis.dbsize()
        print(r)
        time.sleep(5)

if __name__ == "__main__":
    test()

