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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomConsumer {

    private static final Logger log = LoggerFactory.getLogger(CustomConsumer.class);

    private Properties configProperties;
    private final String configFileName = "config.properties";

    /**
     * Default constructor.
     * 
     */
    public CustomConsumer() throws FileNotFoundException, IOException {
	initializeProperties(configFileName);
    }

    /**
     * This method reads all the configuration data from properties file and
     * initializes.
     * 
     * @param fileName
     *            is the properties file that is to be read.
     * 
     * @throws FileNotFoundException, IOException
     */
    public void initializeProperties(String fileName) throws FileNotFoundException, IOException {
	FileInputStream fileInput;
	configProperties = new Properties();
	try {
	    fileInput = new FileInputStream(fileName);
	    configProperties.load(fileInput);
	    fileInput.close();
	} catch (FileNotFoundException e) {
	    log.error("Configuration properties file not found.");
	    throw e;
	} catch (IOException e) {
	    log.error("Failed while reading configuation parameters : {}" + e);
	    throw e;
	}
	log.info("Configured to Kafka Server: {}:{}", configProperties.get("kafka_host"),
		configProperties.get("kafka_port"));
	log.info("Configured to Elastic Search: {}:{}", configProperties.get("elasticSearch_host"),
		configProperties.get("elasticSearch_port"));
    }

    /**
     * This method starts the spark context and will be in listening mode to consume
     * the data. On data receive, it writes same to Elastic Search
     */
    public void startSparkContextConsumer() {
	KafkaTopicConsumer topicConsumer = new KafkaTopicConsumer(configProperties);
	topicConsumer.startConsumer();

    }

}
