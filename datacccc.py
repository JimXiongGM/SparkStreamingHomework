from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev
import pyspark.sql.functions as F

spark = SparkSession.builder.appName(
    "StructuredStreamingWithStatsAndWatermark"
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9876)
    .option("includeTimestamp", True)
    .load()
)

ints = lines.select(col("value").cast("integer").alias("number"), col("timestamp"))

window_duration = "10 seconds"
slide_duration = "5 seconds"
watermark_duration = "5 seconds"

windowedStats = (
    ints.withWatermark("timestamp", watermark_duration)
    .groupBy(F.window("timestamp", window_duration, slide_duration))
    .agg(mean("number").alias("mean"), stddev("number").alias("stddev"))
)

outliers = windowedStats.withColumn(
    "lower_bound", col("mean") - 2 * col("stddev")
).withColumn("upper_bound", col("mean") + 2 * col("stddev"))


def outliers_filter(df):
    return df.filter((df.number < df.lower_bound) | (df.number > df.upper_bound))


outliers_query = (
    outliers.alias("stats")
    .join(
        ints.alias("ints"),
        F.col("stats.window.start") <= F.col("ints.timestamp"),
        "inner",
    )
    .select(
        F.col("ints.timestamp"),
        F.col("ints.number"),
        F.col("stats.mean"),
        F.col("stats.stddev"),
        F.col("stats.lower_bound"),
        F.col("stats.upper_bound"),
    )
)

outliers_filtered = outliers_query.transform(outliers_filter)

query = outliers_filtered.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
