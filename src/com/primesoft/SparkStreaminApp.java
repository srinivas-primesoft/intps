package com.primesoft;

import java.io.FileNotFoundException;
import java.io.IOException;

public class SparkStreaminApp {

    public static void main(String[] args) {
	/*// TODO Auto-generated method stub
	System.out.println("############## Welcome to PrimeSoft Spark Streaming App ##############");
	System.err.println("Your arguments were " + Arrays.asList(args));
	if (args.length < 1) {
	    System.err.println("Usage: SparkStreaminApp -role <Consumer/Producer> -testDataFile <filePath>\n"
		    + "testDataFile parameter is optional for Producer."
		    + "Ex: SparkStreaminApp -role Consumer/Producer -testDataFile c:\test.json \n");
	    System.exit(1);
	}*/

	try {
	    CustomConsumer consumer = new CustomConsumer();
	    consumer.startSparkContextConsumer();
	    System.out.println("############ Custom consumer application started. ############");
	} catch (FileNotFoundException e) {
	    System.out.println(
		    "############ Cannot start the consumer application as the properties file is not visible ############");
	    System.exit(1);
	} catch (IOException e) {
	    System.out.println(
		    "############ Cannot start the consumer application as the properties file cannot be read, check permissions on the file ############");
	    System.exit(2);
	}

    }

}
