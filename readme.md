# note

doc: https://spark.apache.org/docs/latest

```bash
# java 17
wget -c https://download.oracle.com/java/17/archive/jdk-17.0.9_linux-x64_bin.tar.gz
tar -zxvf jdk-17.0.9_linux-x64_bin.tar.gz -C ~/opt
export JAVA_HOME=~/opt/jdk-17.0.9

# spark
# download: https://spark.apache.org/downloads.html
wget -c https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
# scala 2.13?
wget -c https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz
# 解压
tar -zxvf spark-3.5.0-bin-hadoop3.tgz

# test
cd spark-3.5.0-bin-hadoop3
./bin/run-example SparkPi 10
```

流式计算 https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

index 

precision


文件是 follower/cpu1/test1/raft/node1/region000100000000/statemachine.log

test1 - test20 连起来

不同类型的异常方式需要for循环展示

只需要node1即可



完成一个异常检测任务。给定一些log文件，格式如下：
```
operator,term,index,raftTime,prevApplyTime,valueSize
InsertRowNode,1,1,1685462268278,1685462268525,100
InsertRowNode,1,3,1685462268829,1685462268840,100
```
现在需要：
1. 流式数据生成器。将上述log生成流式数据，发送到端口9876。
2. 使用 spark Structured Streaming 接收数据，设定一个时间窗口超参k，计算每一条数据和上一条数据raftTime的时间差，

都使用python实现


端口9876会传来流式数据，格式为 operator,term,index,raftTime,prevApplyTime,valueSize 现在需要使用 spark Structured Streaming 接收数据，设定一个时间窗口超参k，只保留 raftTime 字段，其余丢掉，将接收到的数据转为DataFrame，打印即可

在 Spark Structured Streaming 中，由于流处理的无界性质，我们不能直接访问上一条数据。但是，我们可以使用窗口函数或者聚合函数来处理一组数据。


一个任务，9876端口会一直接受 int 的数据，请

预先计算 rafttime 的差值，生成一个流式数据，发送到某端口，使用 Spark Structured Streaming + python 计算在给定窗口时间k内，每个窗口内的数据的平均值和标准差，并且判断窗口内有没有数值距离mean超过2倍标准差的数据，如果有，打印出来。


一个任务，9876端口会生成一个流式数据，请使用 Spark Structured Streaming + python 计算：给定窗口时间k内，每个窗口内的数据减去给定的a后除以给定的b，如果有数据大于3，打印true。



例如 相减后
2
3
4
5
56
6
6
使用spark聚合后得到 mean = 10, std = 20 然后不能和上面的数字

从Hadoop中读取log并在端口9876上开启数据服务器，模拟流式数据；使用Spark Structured Streaming读取并计算窗口k内的数据的标准差，如果标准差>m，则认为数据波动过大，存在异常，将当前时间戳和标准存到redis中，最后，从redis中取数，使用mattplotlib绘制出来。



