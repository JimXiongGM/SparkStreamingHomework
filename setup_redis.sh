#!/bin/bash

echo "setup redis"
tar xvzf redis-7.2.3.tar.gz
cd redis-7.2.3
make -j 4
src/redis-server --version
src/redis-cli --version

cd /app