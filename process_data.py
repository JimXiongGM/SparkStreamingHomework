import os
from glob import glob

"""
将数据
1. 转换格式
2. 存到log_data目录下
"""

# follower/cpu1/test1/raft/node1/region000100000000/statemachine.log -> log_data/1.log
# follower/cpu1/test2/raft/node1/region000100000000/statemachine.log -> log_data/2.log
os.makedirs("log_data", exist_ok=True)
for i in range(1,21):
    path = f"follower/cpu1/test{i}/raft/node1/region*/statemachine.log"
    path = glob(path)[0]
    with open(path, "r") as f:
        contents = f.readlines()
    new_contents = []
    _last_time = None
    for idx, line in enumerate(contents):
        if idx == 0:
            continue
        line = line.strip().split(",")
        raftTime = int(line[3])
        if _last_time is None:
            _last_time = raftTime
        else:
            diff = raftTime - _last_time
            _last_time = raftTime
            new_contents.append(f"{abs(diff)}\n")
    with open(f"log_data/{i}.log", "w") as f:
        f.writelines(new_contents)
print("done")