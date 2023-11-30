from glob import glob
from tqdm import tqdm
from hdfs import InsecureClient

patt = "follower/cpu1/test*/raft/node1/region000100000000/statemachine.log"
# test 1 - test 20
paths = glob(patt)
paths.sort(key=lambda x: int(x.split("/")[2][4:]))

client = InsecureClient("http://localhost:9870", user="jimx")
client.makedirs("/raftlog/cpu")

# follower/cpu1/test1/raft/node1/region000100000000/statemachine.log -> /raftlog/cpu/test1/statemachine.log
for p in tqdm(paths):
    client.makedirs("/raftlog/cpu/" + p.split("/")[2])
    client.upload("/raftlog/cpu/" + p.split("/")[2] + "/statemachine.log", p)

print(list(client.walk("/raftlog/cpu", depth=2)))
