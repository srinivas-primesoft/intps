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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ElasticSearchManager implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger appLogger = LoggerFactory.getLogger(ElasticSearchManager.class);
	private static final String tableName = "olm_subproduct"; //for demo purpose hard coded to be read from mapping file
	private static String elasticSearchURL;
	private static final String sqlUpdateAction = "UPDATE";
	private static final String primaryKey ="COD_SUBPROD";
	private static final String payLoad ="payload";
	Properties configProperties;
	String elasticSearchHost;
	HttpClient httpClient;
	
	/**
     * Constructor. 
     * 
     */
	public ElasticSearchManager(Properties properties) {
		this.configProperties = properties;
		elasticSearchHost = ""+properties.get("elasticSearch_host")+":"+properties.get("elasticSearch_port");
	}
    
	/**
     * This method is used to write data to ElasticSearch tables. 
     * 
     * @param data record data to be written to ES      
     * 
     * @throws IOException if there is an error. 
     */
    public void sendDataToES(String data) throws Exception {
    	
    	elasticSearchURL = "http://"+elasticSearchHost+"/"+tableName+"/record/";
    	httpClient = HttpClientBuilder.create().build();
		appLogger.info("########## ElasticSearch URL configured to:"+ elasticSearchURL);
		
    	String url = elasticSearchURL;
        JSONObject jsonObject = new JSONObject(data.toString());
        JSONObject payLoadData = (JSONObject) jsonObject.get(payLoad);
    	Map<String,String> recordData = getRecordDataMap(payLoadData);
	    if (!recordData.isEmpty()) {
	    	String actionType = recordData.get("TYPE");
	    	if (sqlUpdateAction.equalsIgnoreCase(actionType)) {
	    		String indexVal = getRecordIndex(recordData.get(primaryKey));
	    		if (indexVal.isEmpty()) {
	    			appLogger.error("Failed to retrieve existing record information for update action.");
	    			return;
	    		}
	    		url = url+indexVal;
	    	}
	    	String response = sendPostRequest(url, payLoadData.toString());
	    	if(response != null) {
	    		appLogger.info("Post request response :"+response);
	    	}
    	}	    	        				
	}	
    
    /**
     * This method sends a HTTP post request to Elastic Search. 
     * 
     * @param data record data to be written to ES with post request      
     * 
     */
    public String sendPostRequest(String url, String data) {
   
    	HttpPost post = new HttpPost(url);
		
		StringEntity entity;
		try {
			entity = new StringEntity(data);
			entity.setContentType((Header) new BasicHeader("Content-Type","application/json"));
		    post.setEntity(entity);
		} catch (UnsupportedEncodingException e) {			
			appLogger.error("Failed while composing entity with exception:"+e.getMessage());
			return null;
		}	    
	    
		HttpResponse response;
		BufferedReader rd;
		StringBuffer result = new StringBuffer();
		String line = "";
		try {
			response = httpClient.execute(post);
			appLogger.info("Response Code : "+ response.getStatusLine().getStatusCode());
			rd = new BufferedReader(
			        new InputStreamReader(response.getEntity().getContent()));
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
		} catch (ClientProtocolException e) {
			appLogger.error("Failed with exception ClientProtocolException.");
			return null;
		} catch (IOException e) {
			appLogger.error("Failed with IOException.");
			return null;
		}
    	return result.toString();
    }
    
    /**
     * This method is to retrieve the ES index using HTTP get request by using primary key. 
     * 
     * @param primaryKeyValue primary key to fetch the correct record from the ES      
     * 
     */
    public String getRecordIndex(String primaryKeyValue) {
    	String indexValue = "";
    	// Url is hard coded with primary key for demo purpose
    	String url = elasticSearchURL+ "_search?q=NBR_STRCID="+primaryKeyValue;
    	String response = sendGetRequest(url);
    	if(response != null) {
    		appLogger.info("Get request response :"+response);    		
    		String idValue = response.substring(response.indexOf("\"_id\":\"")+7);
    		indexValue = idValue.substring(0, idValue.indexOf("\""));
    	}
    	return indexValue;
    }
    
    /**
     * This method sends a HTTP get request to Elastic Search. 
     * 
     * @param url ES host url
     * @return response returns the http get response data on success, otherwise null     
     *  
     */
    public String sendGetRequest(String url) {    	   
    	HttpGet  getRequest = new HttpGet(url);
    	getRequest.addHeader("User-Agent", "Mozilla/5.0");
	    
		HttpResponse response;
		BufferedReader rd;
		StringBuffer result = new StringBuffer();
		String line = "";
		try {
			response = httpClient.execute(getRequest);
			appLogger.info("Response Code : "+ response.getStatusLine().getStatusCode());
			rd = new BufferedReader(
			        new InputStreamReader(response.getEntity().getContent()));
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
		} catch (ClientProtocolException e) {
			appLogger.error("Failed with exception ClientProtocolException.");
			return null;
		} catch (IOException e) {
			appLogger.error("Failed with IOException.");
			return null;
		}
			
    	return result.toString();
    }
    
    /**
     * It generates hash map for the given data. 
     * 
     * @param data on which hash map will be generated 
     * @return response returns the http get response data on success, otherwise null     
     *  
     */    
    public Map<String,String> getRecordDataMap(JSONObject payLoadData) {
    	Set<String> keyItr = payLoadData.keySet();
        Map<String,String> recordData = new HashMap<String,String>();

        for (String key : keyItr) {
            recordData.put(key, payLoadData.get(key).toString());
            appLogger.info("Key :"+key+ "     Value :"+payLoadData.get(key).toString());
        }
    	return recordData;
    }
}
