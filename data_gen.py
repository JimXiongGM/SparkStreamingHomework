import socket
import time
from glob import glob
from tqdm import tqdm


def generate_log_data():
    """
    data format:
        operator,term,index,raftTime,prevApplyTime,valueSize
        InsertRowNode,1,1,1685462268278,1685462268525,100
        InsertRowNode,1,3,1685462268829,1685462268840,100
    """
    paths = glob("follower/cpu1/test*/raft/node1/region000100000000/statemachine.log")
    for p in tqdm(paths, desc="Generating log data"):
        _last_time = None
        with open(p, "r") as f:
            for idx, line in enumerate(f):
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
        break


def start_server(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Listening on {host}:{port}")

    while True:
        try:
            client_socket, client_addr = server_socket.accept()
            print(f"Connection established from {client_addr}")

            # 使用 client_socket.recv() 从客户端接收数据
            for i in generate_log_data():
                client_socket.sendall(i.encode())
                time.sleep(0.05)
        except BrokenPipeError:
            print("Connection closed")
        finally:
            client_socket.close()



if __name__ == "__main__":
    host = "localhost"
    port = 9876
    start_server(host, port)
