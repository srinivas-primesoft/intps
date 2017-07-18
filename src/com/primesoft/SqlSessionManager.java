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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlSessionManager {
	private static final Logger LOG = LoggerFactory.getLogger(SqlSessionManager.class);
	private static final String INSERT_ACTION = "INSERT";
	private static final String UPDATE_ACTION = "UPDATE";
	private static final String TABLE_NAME = "OLM_SOURCE_ACCOUNT_DTLS";
	
	/**
     * Default constructor. 
     * 
     */
	public SqlSessionManager() {
		
	}
	/**
     * This method is used to process the data in SQL context. 
     * 
     * @param recordDataMap data to process in sql context  
     * 
     * @throws IOException if there is an error. 
     */    
    public static void sqlOperation(HashMap<String, String> recordDataMap, String testData) {    	   	      
    	String insertStatement = composeSQLStatement(recordDataMap);
    	System.out.println("########## Statement ="+insertStatement);
    	
    }
    
    /**
     * This method composes the sql statement based on the action type received in the data. 
     * 
     * @param recordDataMap based on this data sql statement will be 
     * composed for the specific action type (insert/update).  
     * 
     * @throws IOException if there is an error. 
     */
	private static String composeSQLStatement(HashMap<String,String> recordDataMap) {
		String statement = "";
		String parameters = "";
		String values = "";
		
		String actionType = recordDataMap.get("TYPE").replace("\"","").trim();
		if(actionType.equals(INSERT_ACTION)) {
			for(Map.Entry m:recordDataMap.entrySet()){
	        	System.out.println("##########"+m.getKey()+" "+m.getValue()+"##########");
	        	String param = " "+m.getKey();	        	
	        	String value = " "+m.getValue();
	        	value= value.replace("\"", "'");
	        	if (!m.getKey().equals("TYPE")) {
	        		if (!values.isEmpty()) { values = values + ","; }
	        		if (!parameters.isEmpty()) { parameters = parameters + ","; }
	        		values = values + value;
	        		parameters = parameters+ param;
	        	}
			}
			statement = "Insert into "+TABLE_NAME+" ("+parameters+") values("+values+");";
		} else if(actionType.equals(UPDATE_ACTION)) {
			statement = "update "+TABLE_NAME+" set "+
					recordDataMap.get("NBR_PRIORITY")+recordDataMap.get("COD_GLDOMICILE")+recordDataMap.get("NBR_INSTRLVL")+recordDataMap.get("FLG_SWEEPTYP")+
					recordDataMap.get("NBR_PERCENT")+recordDataMap.get("AMT_DR_MARGIN")+recordDataMap.get("AMT_CR_MARGIN")+recordDataMap.get("AMT_MAXBAL")+
					recordDataMap.get("AMT_MINBAL")+recordDataMap.get("AMT_DR_MARGIN")+recordDataMap.get("AMT_CR_MARGIN")+recordDataMap.get("AMT_MAXBAL")+
					" where NBR_STRCID ="+recordDataMap.get("NBR_STRCID")+", NBR_INSTRID="+recordDataMap.get("NBR_INSTRID")+";";
		} else {
			LOG.error("This operation is not supported at this moment.");
			return null;
		}
		
		return statement;
	}
}
