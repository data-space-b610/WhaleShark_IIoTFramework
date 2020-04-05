#!/bin/bash
sudo service ssh restart
sudo service postgresql restart

bash /home/csle/zookeeper-3.4.9/bin/zkServer.sh start
ssh csle@master -p 2243 "cd /home/csle/zookeeper-3.4.9/bin; ./zkServer.sh start"
ssh csle@csle1  -p 2243 "cd /home/csle/zookeeper-3.4.9/bin; ./zkServer.sh start"
start-dfs.sh
start-yarn.sh
start-hbase.sh
/home/csle/kafka/bin/kafka-server-start.sh /home/csle/kafka/config/server.properties &
/home/csle/ui_of_csle/apache-tomcat-7.0.81/bin/catalina.sh start &
/home/csle/start-mongo.sh &
sleep 5
cd /home/csle/ksb-csle/bin
/home/csle/ksb-csle/bin/startKnowledge_service.sh localhost 9876
