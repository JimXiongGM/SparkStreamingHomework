from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode
# from pyspark.sql.functions import split

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Split the lines into words
# words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Assuming that lines is a DataFrame with a single string column "value" that contains the CSV data
# words = lines.select(split(lines.value, ",")[1].alias("word"))

# Generate running word count
# wordCounts = words.groupBy("word").count()

# Start running the query that prints the running counts to the console
# query = wordCounts.writeStream.outputMode("complete").format("console").start()

# query.awaitTermination()

from pyspark.sql.functions import split, current_timestamp


"""
./bin/spark-submit ../structured_network_wordcount.py localhost 9999
"""

from pyspark.sql.functions import lag, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.functions import window

# Assuming that lines is a DataFrame with a single string column "value" that contains the CSV data
# words = lines.select(split(lines.value, ",")[1].alias("word"))

# Assuming that lines is a DataFrame with a single string column "value" that contains the CSV data
words = lines.select(split(lines.value, ",")[1].alias("word"), current_timestamp().alias("timestamp"))


# Define a window specification
windowSpec = Window.orderBy("timestamp")  # Assuming that there is a "timestamp" column

# Add a new column "previous_word" that contains the word from the previous row
words = words.withColumn("previous_word", lag("word").over(windowSpec))

# Concatenate the current word and the previous word
words = words.withColumn("concatenated_word", concat_ws(" ", "previous_word", "word"))

windowedWords = words.groupBy(window(words.timestamp, "3 seconds"), words.word).count()

# Start running the query that prints the running counts to the console
query = windowedWords.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()


# Assuming that words is a DataFrame with columns "word" and "timestamp"
# windowedWords = words.groupBy(window(words.timestamp, "1 minute"), words.word).count()
