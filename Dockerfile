FROM ubuntu:22.04

SHELL ["/bin/bash", "-c"]

WORKDIR /app

COPY . /app

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
    screen ssh openssh-server pdsh vim iputils-ping telnet \
    python3 python3-pip

# hadoop
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV PDSH_RCMD_TYPE=ssh
ENV USER=root

RUN echo "export PDSH_RCMD_TYPE=ssh" >> /etc/profile && . /etc/profile && \
    service ssh start

RUN pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

RUN bash setup_hadoop.sh
RUN bash setup_spark.sh
RUN bash setup_redis.sh

CMD ["bash", "run.sh"]
