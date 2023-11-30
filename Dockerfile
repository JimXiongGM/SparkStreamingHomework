FROM ubuntu:latest

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    screen ssh pdsh

RUN pip3 install -r requirements.txt

RUN bash setup_hadoop.sh
RUN bash setup_spark.sh
RUN bash setup_redis.sh

CMD ["bash", "run.sh"]
