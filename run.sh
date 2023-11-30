#!/bin/bash

# start hadoop and redis
echo "start hadoop and redis"
cd /app/hadoop-3.3.6
sbin/start-dfs.sh

cd /app
cd redis-7.2.3
screen -S redis -d -m
screen -S redis -X stuff "src/redis-server --port 6379
"

# run the calculation
echo "start running the calculation"

# 1. upload data to HDFS
python3 step1_hdfs.py

# 2. start data server
screen -S dataserver -d -m
screen -S dataserver -X stuff "python3 step2_datagen.py
"

# 3. start Spark Structured Streaming：
cd /app/spark-3.5.0-bin-hadoop3
screen -S spark -d -m
screen -S spark -X stuff "bin/spark-submit ../step3_spark.py
"

# 4. wait for 30min, start data visualization
sleep 1800
python3 step4_redis.py