#!/bin/bash

echo "setup spark"
# java 17
tar xvzf jdk-17.0.9_linux-x64_bin.tar.gz
export JAVA_HOME=/app/jdk-17.0.9

# spark
tar xvzf spark-3.5.0-bin-hadoop3.tgz

# test
cd spark-3.5.0-bin-hadoop3
./bin/run-example SparkPi 10