Environment Setup

This application requires the following
1. Oracle 11g R2
2. DBVisit Replicate 2.9.00 (http://www.dbvisit.com/products/dbvisit_replicate_real_time_oracle_database_replication/)
3. DBVisit Connector for Kafka 2.9.00
4. Confluent 3.2.2 (https://www.confluent.io/product/confluent-platform/)
5. Elastic Search

Running the applicatin
The following are steps
1. Ensure Oracle database is in Archivelog mode. To place Oracle in archivelog mode refer to 
http://www.oracledistilled.com/oracle-database/backup-and-recovery/enabledisable-archive-log-mode-10g11g/

2. Start the mine process using DBVisit replicate 
TODO

3. Start Elastic Search
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

4. Start Kafka server
a) Zookeeper
CONFLUENT_HOME/bin/zookeeper-server-start CONFLUENT_HOME/etc/kafka/zookeeper.properties

b) Kafka Server
CONFLUENT_HOME/bin/kafka-server-start CONFLUENT_HOME/etc/kafka/server.properties

c)Schema Registry
CONFLUENT_HOME/bin/schema-registry-start CONFLUENT_HOME/etc/schema-registry/schema-registry.properties

5. Run DBVisit Connector for Kafka
CONFLUENT_HOME/bin/connect-standalone CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties ./etc/kafka-connect-dbvisitreplicate/dbvisit-replicate.properties

6. Run the application after checking out the code and opening in eclipse. Run SparkStreamingApp as java application 
Spark caveat while running on windows
**************** Spark ****************
We need winutil.exe though we are not using hadoop
  https://issues.apache.org/jira/browse/SPARK-2356
  set HADOOP_HOME/HADOOP_CONF










