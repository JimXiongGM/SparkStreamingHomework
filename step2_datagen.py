import socket
import time
from tqdm import tqdm
from hdfs import InsecureClient


def generate_log_data():
    """
    data format:
        operator,term,index,raftTime,prevApplyTime,valueSize
        InsertRowNode,1,1,1685462268278,1685462268525,100
        InsertRowNode,1,3,1685462268829,1685462268840,100
    880110 data

    """
    # paths = glob("follower/cpu1/test*/raft/node1/region000100000000/statemachine.log")

    client = InsecureClient("http://localhost:9870", user="jimx")
    paths = client.list("/raftlog/cpu/")
    pbar = tqdm(paths, desc="Generating log data", total=880110)
    for _p in paths:
        with client.read(
            f"/raftlog/cpu/{_p}/statemachine.log", encoding="utf-8"
        ) as reader:
            contents = reader.read()
        contents = contents.splitlines()
        _last_time = None
        for idx, line in enumerate(contents):
            pbar.update(1)
            if idx == 0:
                continue
            line = line.strip().split(",")
            raftTime = int(line[3])
            if _last_time is None:
                _last_time = raftTime
            else:
                diff = raftTime - _last_time
                _last_time = raftTime
                yield f"{diff}\n"
        # break


def start_server(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Listening on {host}:{port}")

    while True:
        try:
            client_socket, client_addr = server_socket.accept()
            print(f"Connection established from {client_addr}")
            # 每秒500数据
            for i in generate_log_data():
                client_socket.sendall(i.encode())
                time.sleep(0.002)
        except BrokenPipeError:
            print("Connection closed")
        finally:
            client_socket.close()
            pass


def test_data():
    c = 0
    for i in generate_log_data():
        c += 1
    print(c)


if __name__ == "__main__":
    # test_data()

    host = "localhost"
    port = 9876
    start_server(host, port)
