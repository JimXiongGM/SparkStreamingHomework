from hdfs.client import Client

"""
pip install hdfs -U
"""
client = Client("http://localhost:9870")
client.list("/")

# client.makedirs("/test")
# client.makedirs("/test", permision=777)  # permision可以设置参数

# client.rename("/test", "/test123")  # 将/test 目录改名为123
# client.delete("/test", True)  # 第二个参数表示递归删除

with client.read("/user/jimx/output/part-r-00000") as reader:
    print(reader.read().decode('utf-8'))
