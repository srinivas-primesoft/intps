package com.primesoft;

import java.io.FileNotFoundException;
import java.io.IOException;

public class SparkStreamingApp {

    public static void main(String[] args) {

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
