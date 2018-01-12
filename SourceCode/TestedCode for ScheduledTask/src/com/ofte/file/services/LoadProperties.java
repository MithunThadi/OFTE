package com.ofte.file.services;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 * 
 * Class Functionality:
 * 					This class is used to return the properties	of kafka,Casasandra,Zookeeper.
 * Methods:
 * 			public Properties loadProperties(String fileName)
 * 			public Properties getKafkaProperties()
 * 			public Properties getSecondLayerProperties()
 * 			public Properties getCassandraProperties()
 * 			public Properties getOFTEProperties()
 *
 */
public class LoadProperties {
	/**
	 * This method is used to return the properties	of kafka,Casasandra,Zookeeper
	 * @param fileName
	 * @return properties
	 */
	public Properties loadProperties(String fileName) {
		//Creating an object for InputStream class
		InputStream inputStream = LoadProperties.class.getResourceAsStream(fileName);
		//Creating an object for Properties class
		Properties properties = new Properties();
		try {
			properties.load(inputStream);
		} 
		//catching the exception for IOException
		catch (IOException e) {
			e.printStackTrace();
		}
		//return statement
		return properties;
	}
	/**
	 * This method is used to load the kafka.properties file
	 * @return loadProperties("Kafka.properties")
	 */
	public Properties getKafkaProperties() {
		//return statement
		return loadProperties("Kafka.properties");

	}
	/**
	 * This method is used to load the SecondLayer.properties file
	 * @return loadProperties("SecondLayer.properties")
	 */
	public Properties getSecondLayerProperties() {
		//return statement
		return loadProperties("SecondLayer.properties");

	}
	/**
	 * This method is used to load the Cassandra.properties file
	 * @return loadProperties("Cassandra.properties")
	 */
	public Properties getCassandraProperties() {
		//return statement
		return loadProperties("Cassandra.properties");

	}
	/**
	 * This method is used to load the OFTE.properties file
	 * @return loadProperties("OFTE.properties")
	 */
	public Properties getOFTEProperties() {
		//return statement
		return loadProperties("OFTE.properties");

	}
}
