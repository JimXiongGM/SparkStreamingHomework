from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, current_timestamp, window, mean
from pyspark.sql.types import StringType, StructField, StructType
import pyspark.sql.functions as F
from pyspark.sql.functions import when, collect_list, array_contains, expr

# 创建Spark会话
spark = SparkSession.builder.appName("StructuredStreamingAverageStddev").getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("ERROR")

# 指定窗口时长k
k = "5 seconds"  # 示例为10秒的窗口

# 定义输入数据的架构
schema = StructType([StructField("value", StringType())])

# 创建输入流
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9876)
    .load()
)

ints = lines.select(col("value").alias("number")).selectExpr("CAST(number AS INTEGER)")

# 转换数据类型，添加时间戳
# int_values_with_timestamp = (
#     lines.withColumn("number", col("value").cast("integer"))
#     .withColumn("timestamp", current_timestamp())
#     .select("number", "timestamp")
# )

# 使用窗口函数计算平均值和标准差
# windowedStats = int_values_with_timestamp.groupBy(
#     window(col("timestamp"), k)  # 使用窗口函数，每k时间计算一次
# ).agg(
#     avg("number").alias("avg"),  # 计算窗口内数字的平均值
#     stddev("number").alias("stddev"),  # 计算窗口内数字的标准差
# )

# outliers = windowedStats \
#     .where(
#         (abs(col('number') - col('mean')) > 2 * col('stddev'))  # 找到异常值
#     ) \
#     .select(
#         col('window'),
#         col('number'),
#         col('mean'),
#         col('stddev')
#     )

# 定义窗口操作
from pyspark.sql import functions as F

window_duration = "10 seconds"
windowedCounts = ints.groupBy(F.window(F.current_timestamp(), window_duration)).agg(
    mean("number").alias("mean"),
    stddev("number").alias("stddev"),
    collect_list("number").alias("numbers_in_window"),
)

outliers = (
    windowedCounts.withColumn("lower_bound", col("mean") - 2 * col("stddev"))
    .withColumn("upper_bound", col("mean") + 2 * col("stddev"))
    .withColumn("number", col("numbers_in_window"))
#     .withColumn(
#     "comparison_result",
#     expr("transform(col(\"numbers_in_window\"), x -> CASE WHEN x > {} THEN 'Above threshold' WHEN x < {} THEN 'Below threshold' ELSE 'Equals threshold' END)".format(col("upper_bound"), col("lower_bound"))
#     )
# )

#     .withColumn(
#     "comparison_result",
#     expr(
#         "transform(number, x -> "
#         "CASE WHEN x > {} THEN 'Above threshold' "
#         "     WHEN x < {} THEN 'Below threshold' "
#         "     ELSE 'Equals threshold' "
#         "END)".format(col("upper_bound"), col("lower_bound"))
#     )
# )
    # .withColumn("lower_bound_check", when(), "positive").otherwise("non-positive"))
    # .select("window", "mean", "stddev", "lower_bound", "upper_bound", "new_col")
    # .join(
    #     ints,
    #     F.col("number").between(F.col("lower_bound"), F.col("upper_bound")),
    #     "inner",
    # )
)  # 将原始数据与界限进行比较


# processed_df = df.withColumn("processed_numbers", 
#                              when(col("numbers").isNotNull(), 
#                                   array(*[lit(elem) for elem in process_array(col("numbers"))]))
#                              .otherwise(col("numbers")))


# 输出到控制台
query = outliers.writeStream.outputMode("complete").format("console").start()


query.awaitTermination()  # 等待流处理终止
