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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class SparkAvroConsumer {

	public static String sampleDataFileName = "users.avro";
	
	private static final Charset encoding_charSet = Charset.forName("UTF-8");
	private static final Logger appLogger = LoggerFactory.getLogger(SparkAvroConsumer.class);

    public static void main(String[] args) throws IOException {
    	appLogger.info("############ Spark consumer application started. ############");
    	String topicName = "test";
        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(500));

        Set<String> topics = Collections.singleton(topicName);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);
        
        //creates a new .avro file 
        initFile(sampleDataFileName);
        
        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(avroRecord -> {
            	Schema schema = null;
        		try {
        			schema = new Schema.Parser().parse(new File("user.avsc"));
        		} catch (IOException e) {
        			appLogger.error("Failed while parsing the .avsc schema file.");
        		}
        		
            	Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);            	
                GenericRecord record = recordInjection.invert(avroRecord._2).get();
                sendDataToES(record.toString());
                                
                String jsonOutput = getJsonString(record);
                appLogger.info("AVRO Record :"+record.toString());
                appLogger.info("Json Record :"+jsonOutput);
                
                FileWriter fw = new FileWriter(sampleDataFileName, true);
                fw.write(jsonOutput+",\n");
                fw.close();
            });
        });

        ssc.start();
        try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			appLogger.warn("App terminated with exception :", e);
		}
    }    
    
    /**
     * This method is used to write data to ElasticSearch tables. 
     * 
     * @param data employee record data      
     * 
     * @throws IOException if there is an error. 
     */
    public static void sendDataToES(String data) throws Exception{
	    
    	//This hardcoding has to be removed
    	String url = "http://192.168.5.157:9200/employee/record";
    	
		HttpClient client = HttpClientBuilder.create().build();
		HttpPost post = new HttpPost(url);
	
		// add header
		post.addHeader("content-type", "application/json");
			
		StringEntity entity = new StringEntity(data);
	    entity.setContentType((Header) new BasicHeader("Content-Type","application/json"));
	    post.setEntity(entity);
	
		HttpResponse response = client.execute(post);
		appLogger.info("Response Code : "+ response.getStatusLine().getStatusCode());
	
		BufferedReader rd = new BufferedReader(
		        new InputStreamReader(response.getEntity().getContent()));
	
		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		
		appLogger.info("Post request response :"+result.toString());	
	}	
    
    /**
     * This method creates a file. 
     * 
     * @param fileName is the file to be created, if it already 
     * exists it deletes then creates. 
     * 
     * @throws IOException if there is an error. 
     */    
    public static void initFile(String fileName) {
    	Path filePath = Paths.get(fileName);
    	try {	    	    		
			Files.deleteIfExists(filePath);				    	
	    	Files.createFile(filePath);
	    	appLogger.info(fileName+" created successfully.");
    	} catch (IOException e) {
			appLogger.error("Failed to create "+fileName+" file.");
		}    	      
    }
           
    /**
     * Returns an encoded JSON string for the given Avro object. 
     * 
     * @param record is the record to encode 
     * @return the JSON string representing this Avro object. 
     * 
     * @throws IOException if there is an error. 
     */ 
    public static String getJsonString(GenericContainer record) throws IOException { 
      ByteArrayOutputStream os = new ByteArrayOutputStream(); 
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), os); 
      DatumWriter<GenericContainer> writer = new GenericDatumWriter<GenericContainer>(); 
   
      writer.setSchema(record.getSchema()); 
      writer.write(record, encoder); 
      encoder.flush(); 
      String jsonString = new String(os.toByteArray(), encoding_charSet); 
      os.close(); 
      return jsonString; 
    }
    
    /**
     * This method is to read the AVRO format data and save as json file 
     * 
     * @param inputFileName .avro file as an input
     * @param jsonFileName .json file to be created  
     * 
     */
    public static void saveAsJsonFile(String inputFileName, String jsonFileName){
        String line = null;
        String fileContent = "";
        try {
            FileReader fileReader = new FileReader(inputFileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                fileContent+= line;
            }   
            bufferedReader.close();         
            
            FileWriter fileWriter = new FileWriter(jsonFileName);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write("[ " + fileContent.substring(0, fileContent.length()-1) + "\n ]");
            // Always close files.
            bufferedWriter.close();
            
        }
        catch(FileNotFoundException ex) {
        	appLogger.error("'" + inputFileName + "' or '"+""+jsonFileName+ " not found.");                
        }
        catch(IOException ex) {
            appLogger.error("Error in reading/writing file.");                  
        }
        
    }
}
