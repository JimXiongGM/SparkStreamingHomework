from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    stddev,
    avg,
    window,
    current_timestamp,
    when,
    collect_list,
)

spark = SparkSession.builder.appName("StreamingApp").getOrCreate()

# 创建一个用于模拟数据的数据流，这里用 socketTextStream 模拟接收 int 数据
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9876)
    .load()
)

# 将接收到的数据转换为 int 类型
data = lines.select(col("value").cast("int").alias("number"))
data_with_timestamp = data.withColumn("timestamp", current_timestamp())
data_with_window = data_with_timestamp.withColumn(
    "window", window("timestamp", "5 seconds", "5 seconds")
)

# 定义窗口时间和窗口操作
windowed_data = data_with_window.groupBy("window").agg(
    avg("number").alias("average"), stddev("number").alias("stddev")
)

# 计算超过2倍标准差的数据
result = data_with_timestamp.join(
    windowed_data,
    windowed_data.window.start == data_with_timestamp.timestamp.window.start,
).filter((col("number") - col("avg")) > 2 * col("stddev"))

# 打印超过2倍标准差的数据
query = result.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
