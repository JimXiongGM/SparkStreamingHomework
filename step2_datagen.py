import os
import socket
import time

from hdfs import InsecureClient
from tqdm import tqdm


def generate_log_data():
    client = InsecureClient("http://localhost:9870", user=os.environ["USER"])
    paths = client.list("/log_data")
    pbar = tqdm(paths, desc="Generating log data", total=880110)
    for _p in paths:
        with client.read(f"/log_data/{_p}", encoding="utf-8") as reader:
            contents = reader.read()
        contents = contents.splitlines()
        for line in contents:
            pbar.update(1)
            line = int(line.strip())
            yield f"{line}\n"


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
