package com.primesoft;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaTopicConsumer implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConsumer.class);
    private Properties configProperties;
    private String topicName;
   // private final Pattern payLoadPattern = Pattern.compile("payload.*[}]");
    private ElasticSearchManager esManager = null;

    public KafkaTopicConsumer(Properties configProperties) {
	this.configProperties = configProperties;
	topicName = (String) configProperties.get("kafka_topic");
	
	esManager = new ElasticSearchManager(configProperties);
    }
    
    public void startConsumer() {
	String kafkaHost = configProperties.get("kafka_host") + ":" + configProperties.get("kafka_port");
	SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));

	JSONObject topicsjson = new JSONObject(topicName);
	JSONArray topicsList = topicsjson.getJSONArray("topics");
	Set<String> topics = new HashSet<>();
	for (Object topic : topicsList) {
		topics.add(topic.toString());
	}
	Map<String, String> kafkaParams = new HashMap<>();
	kafkaParams.put("metadata.broker.list", kafkaHost);

	JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
		String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
	log.info("Spark subscribed to the topic: {}", topicName);

	JavaDStream<String> json = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
	    private static final long serialVersionUID = 1L;

	    public String call(Tuple2<String, String> message) throws Exception {
		return message._2();
	    };
	});

	json.foreachRDD(rdd -> {
	    rdd.foreach(record -> {
		/*Matcher matcher = payLoadPattern.matcher(record);
		// check all occurrence
		if (matcher.find()) {
		    log.debug("Pattern matched: {}", matcher.group());
		    String payLoadData = matcher.group();
		    payLoadData = payLoadData.substring(payLoadData.indexOf("{"), payLoadData.indexOf("}") + 1);*/
		    log.debug("############## Payload: {}", record);
		    esManager.sendDataToES(record);
		//}
	    });
	});

	ssc.start();
	try {
	    ssc.awaitTermination();
	} catch (InterruptedException e) {
	    log.error("App terminated with an exception: {}", e);
	}
    }

}
