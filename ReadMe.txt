In order to execute the application, we have to setup the environment first. Below are the softwares and their configurations.

**************** Kafka ****************

https://kafka.apache.org/quickstart

1. Download Kakfa

2. Download gradle and build using "gradle jarAll" command.

Start Kafka byusing below commands:

A. Zookeeper
KAFKA_HOME\bin\windows\zookeeper-server-start.bat config\zookeeper.properties

B. Server
KAFKA_HOME\bin\windows\kafka-server-start.bat config\server.properties

Optional:
create a topic
KAFKA_HOME\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic psiTest
list the topics
KAFKA_HOME\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181

producer
KAFKA_HOME\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test

consumer
KAFKA_HOME\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning

*************** Elastic Search **************************

downloadable MSI package or dump (zip)

https://github.com/rgl/elasticsearch-setup/releases

1. download logstash
2. goto bin folder and run this "logstash-plugin install logstash-input-jdbc" command
3. create config file and place it at root folder
4. logstash.bat -f ..\logstash.conf

Check the ES status:
http://localhost:9200/
to know cluster details
http://localhost:9200/_status?pretty=true

**************** Spark ****************

We need winutil.exe though we are not using hadoop
  https://issues.apache.org/jira/browse/SPARK-2356
  set HADOOP_HOME/HADOOP_CONF





