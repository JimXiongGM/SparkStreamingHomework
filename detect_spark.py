from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, avg, window, current_timestamp,when,collect_list
from pyspark.sql.types import IntegerType
import logging

logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.appName("StreamingCalculations").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9876)
    .load()
)

data = lines.select(col("value").cast(IntegerType()).alias("data"))


data_with_timestamp = data.withColumn("timestamp", current_timestamp())

# windowDuration：定义了窗口的长度。在这个例子中，窗口的长度是10秒。这意味着，窗口操作会在最近的5秒的数据上进行。
windowDuration = "5 seconds"
# slideDuration：定义了窗口滑动的频率。在这个例子中，每10秒，窗口就会向前滑动一次，然后在新的窗口上进行操作。
slideDuration = "5 seconds"

data_with_window = data_with_timestamp.withColumn(
    "window", window("timestamp", windowDuration, slideDuration)
)

result = data_with_window.groupBy("window").agg(
    avg("data").alias("average"), stddev("data").alias("stddev"),collect_list("data").alias("numbers_in_window"),
)

# query = (
#     result.writeStream.outputMode("complete")
#     .format("console")
#     .start()
# )

# query.awaitTermination()


# Add a new column 'z_score' to the DataFrame
# result_with_z_score = result.withColumn(
#     "z_score", (col("numbers_in_window") - col("average")) / col("stddev")
# )

# Add a new column 'is_outlier' to the DataFrame
# result_with_outliers = result_with_z_score.withColumn(
#     "is_outlier", when(col("z_score") > 3, True).otherwise(False)
# )

# fff = open("tttttttt.txt","a")

def check_threshold(row):
    # print("hello")
    # mean_val = row["mean"]
    # stddev_val = row["stddev"]
    numbers = row["numbers_in_window"]
    logging.info("sdagfhgdfsdffjkjfsdz", numbers)
    # numbers_list = []
    # for number in numbers:
        
    #     # fff.write(str(number))
    #     if abs(number - mean_val) > 2 * stddev_val:
    #         print(f"Value {number} exceeds threshold in this window.")
    #         # numbers_list.append(number)

# print("sdfwerttfgyjgjh2343546678")
# print("hello")
# mean_val = result.select(col("mean"))
# stddev_val = result.select(col("stddev"))
# numbers = result.select(col("numbers_in_window"))
# logging.info("sdagfhgdfsdffjkjfsdz", numbers)
# numbers_list = []
# for number in numbers:
    
    # fff.write(str(number))
    # if abs(number - mean_val) > 2 * stddev_val:
        # print(f"Value {number} exceeds threshold in this window.")
        # numbers_list.append(number)


query = (

    result.writeStream.foreach(check_threshold).outputMode("complete")
    .format("console")
    .save("tmp1")
    .start()
)

query.awaitTermination()

stream_df = spark.read.format("parquet").load("tmp1")

# 展示读取的 DataFrame
stream_df.show()

# fff.close()
