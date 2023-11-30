import datetime
import time

import matplotlib.pyplot as plt
from redis import StrictRedis

redis = StrictRedis(host="localhost", port=6379, decode_responses=True, db=0)


def test():
    while True:
        r = redis.dbsize()
        print(r)
        time.sleep(5)


def plotdata():
    keys = redis.keys()
    values = [redis.get(key) for key in keys]

    # Convert keys to datetime and values to float
    dates = [datetime.datetime.strptime(key, "%Y-%m-%d %H:%M:%S") for key in keys]
    values = [float(value) for value in values]

    # Create color list
    # red dots is the outliers.
    colors = ["red" if value > 20 else "blue" for value in values]

    plt.scatter(dates, values, c=colors)
    plt.title("CPU Latency")
    plt.savefig("output/cpu_latency.png")


if __name__ == "__main__":
    # test()

    plotdata()
