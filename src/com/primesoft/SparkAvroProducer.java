/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.primesoft;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkAvroProducer {
	
	private static final Logger appLogger = LoggerFactory.getLogger(SparkAvroProducer.class);
	private static final String schemaFile = "user.avsc";

    public static void main(String[] args) throws InterruptedException {
    	appLogger.info("############ Spark producer application started. ############");
    	
    	String topicName = "test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        

        Schema schema = null;
		try {
			schema = new Schema.Parser().parse(new File(schemaFile));
		} catch (IOException e) {
			appLogger.error("Failed while parsing the "+schemaFile+" schema file.");
			System.exit(1);
		}
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        
        appLogger.info("Before data publishing on to the topic:"+topicName);
        for (int i = 6; i < 10; i++) {        	        	
        	
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("name", i + " - name");
            avroRecord.put("gender", "gender value -" + i);
            avroRecord.put("age", i+20);

            byte[] bytes = recordInjection.apply(avroRecord);            
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, bytes);
            producer.send(record);
            appLogger.info("Record "+i+" published successfully.");
            Thread.sleep(400);

        }
        appLogger.info("Data publishing completed for the topic:"+topicName);
        producer.close();
        appLogger.info("############ Spark producer application exited. ############");
    }
}
