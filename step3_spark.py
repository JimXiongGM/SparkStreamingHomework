from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, window, current_timestamp
from pyspark.sql.types import IntegerType
import logging
from redis import StrictRedis

logging.basicConfig(level=logging.WARN)

"""
bin/spark-submit ../step3_spark.py
"""

redis = StrictRedis(host="localhost", port=6379, decode_responses=True, db=1)

spark = SparkSession.builder.appName("StreamingCalculations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9876)
    .load()
)
data = lines.select(col("value").cast(IntegerType()).alias("data"))
data_with_timestamp = data.withColumn("timestamp", current_timestamp())

# windowDuration 定义了窗口的长度。窗口操作会在最近的5秒的数据上进行。
windowDuration = "3 seconds"
# slideDuration 定义了窗口滑动的频率。每xx秒，窗口就会向前滑动一次，然后在新的窗口上进行操作。
slideDuration = "3 seconds"

data_with_timestamp = data_with_timestamp.withWatermark("timestamp", slideDuration)
data_with_window = data_with_timestamp.withColumn(
    "window", window("timestamp", windowDuration, slideDuration)
)
result = data_with_window.groupBy("window").agg(stddev("data").alias("stddev"))
def write_to_redis(df, epoch_id):
    pandas_df = df.toPandas()
    for index, row in pandas_df.iterrows():
        redis.set(str(row["window"][0]), str(row["stddev"]))
query = (
    result.writeStream.foreachBatch(write_to_redis)
    .outputMode("append")
    .start()
)
query.awaitTermination()
