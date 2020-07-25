cd /usr/local 

sleep 1

start-dfs.sh

sleep 15

start-yarn.sh

sleep 15

hdfs dfs -ls /user/hduser

sleep 10

hadoop jar /home/skywalker/Desktop/demo.jar

sleep 25


