#!/bin/bash
stop-hbase.sh
bash /home/csle/kafka/bin/kafka-server-stop.sh
stop-yarn.sh
stop-dfs.sh
bash /home/csle/zookeeper-3.4.9/bin/zkServer.sh stop
/home/csle/ui_of_csle/apache-tomcat-7.0.81/bin/catalina.sh stop
/home/csle/stop-mongo.sh
cd /home/csle/ksb-csle/bin
/home/csle/ksb-csle/bin/stopKnowledge_service.sh
sudo service postgresql stop
