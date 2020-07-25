#!/bin/sh


trap '
  trap - INT # restore default INT handler
  kill -s INT "$$"
' INT

nohup /home/skywalker/Kafka/kafka_2.11-2.3.0/bin/zookeeper-server-start.sh /home/skywalker/Kafka/kafka_2.11-2.3.0/config/zookeeper.properties > zookeeper.log 2>&1 & sleep 30
nohup /home/skywalker/Kafka/kafka_2.11-2.3.0/bin/kafka-server-start.sh /home/skywalker/Kafka/kafka_2.11-2.3.0/config/server.properties > server.log 2>&1 & sleep 30



/usr/bin/python3.6 /home/skywalker/Kafka/kafka_2.11-2.3.0/backup/consumer.py  consumer.log 2>&1 & sleep 30 &
/usr/bin/python3.6 /home/skywalker/Kafka/kafka_2.11-2.3.0/producer.py  producer.log 2>&1  & sleep 30 &
/usr/bin/python3.6 /home/skywalker/Kafka/kafka_2.11-2.3.0/flask-pyspark/connector-pyspark-mongodb.py  pyspark.log 2>&1 sleep 30


