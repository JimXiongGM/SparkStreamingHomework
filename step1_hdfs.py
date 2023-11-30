import os
from glob import glob

from hdfs import InsecureClient
from tqdm import tqdm

patt = "log_data/*.log"
paths = glob(patt)
paths.sort(key=lambda x: int(x.split("/")[-1].split(".")[0]))

client = InsecureClient("http://localhost:9870", user=os.environ["USER"])
client.makedirs("/log_data")

for p in tqdm(paths):
    client.upload("/log_data/" + p.split("/")[-1], p)

print(list(client.walk("/log_data", depth=1)))
