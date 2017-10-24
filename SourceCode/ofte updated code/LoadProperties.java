package com.ofte.services;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 * 
 * Class Functionality:
 * 
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
	 * 
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
	 * 
	 * @return loadProperties("Kafka.properties")
	 */
	public Properties getKafkaProperties() {
		//return statement
		return loadProperties("Kafka.properties");

	}
	/**
	 * 
	 * @return loadProperties("SecondLayer.properties")
	 */
	public Properties getSecondLayerProperties() {
		//return statement
		return loadProperties("SecondLayer.properties");

	}
	/**
	 * 
	 * @return loadProperties("Cassandra.properties")
	 */
	public Properties getCassandraProperties() {
		//return statement
		return loadProperties("Cassandra.properties");

	}
	/**
	 * 
	 * @return loadProperties("OFTE.properties")
	 */
	public Properties getOFTEProperties() {
		//return statement
		return loadProperties("OFTE.properties");

	}
}
