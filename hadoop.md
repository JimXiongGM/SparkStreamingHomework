
```bash
# java
sudo apt-get install -y ssh pdsh
export JAVA_HOME=~/opt/jdk1.8.0_261

# download
wget -c https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.3.6.tar.gz
tar xvzf hadoop-3.3.6.tar.gz
cd hadoop-3.3.6
echo 'export JAVA_HOME=~/opt/jdk1.8.0_261' >> etc/hadoop/hadoop-env.sh
bin/hadoop

# Pseudo-Distributed Operation

# etc/hadoop/core-site.xml:
<configuration>
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
# etc/hadoop/hdfs-site.xml:
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>

# ssh
sudo apt-get install pdsh
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
export PDSH_RCMD_TYPE=ssh
pdsh -q -w localhost

# Execution
bin/hdfs namenode -format
sbin/start-dfs.sh

# see: http://localhost:9870/

# demo
bin/hdfs dfs -mkdir -p /user/jimx
bin/hdfs dfs -mkdir input
bin/hdfs dfs -put etc/hadoop/*.xml input
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar grep input output 'dfs[a-z.]+'
bin/hdfs dfs -cat output/*

# stop
sbin/stop-dfs.sh
```



