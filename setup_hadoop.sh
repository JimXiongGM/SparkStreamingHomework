#!/bin/bash

echo "setup hadoop"

# java
tar xvzf jdk-8u202-linux-x64.tar.gz

# download
# wget -c https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.3.6.tar.gz
tar xvzf hadoop-3.3.6.tar.gz
cd hadoop-3.3.6
echo 'export JAVA_HOME=/app/jdk1.8.0_202' >> etc/hadoop/hadoop-env.sh

# Pseudo-Distributed Operation
# etc/hadoop/core-site.xml:
echo "<configuration>
   <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
   </property>
</configuration>" > etc/hadoop/core-site.xml
# etc/hadoop/hdfs-site.xml:
echo '<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>' > etc/hadoop/hdfs-site.xml

# ssh
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ''
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
pdsh -q -w localhost

# Execution
bin/hdfs namenode -format

cd /app