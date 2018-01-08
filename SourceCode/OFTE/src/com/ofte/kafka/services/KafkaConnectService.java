package com.ofte.kafka.services;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.ofte.file.services.LoadProperties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
/**
 * 
 * Class Functionality:
 * 						The main functionality of this class is depending upon the number of partitions creation the kafka topics dynamically by using zookeeper 
 * 
 * Methods:
 * 			public void createTopic(String topicName, int numberOfPartitions, int numberOfReplications)
 * 			
 *
 */
public class KafkaConnectService {
	//Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	//Creating Logger object for KafkaConnectService class
	Logger logger = Logger.getLogger(KafkaConnectService.class.getName());
	//Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	
	/**
	 * This method creates a topic similar to the file transferred based on the number of partitions and number of replications
	 * @param topicName
	 * @param numberOfPartitions
	 * @param numberOfReplications
	 */
	public void createTopic(String topicName, int numberOfPartitions, int numberOfReplications) {
		//Declaration of parameter ZkClient and initialising it to null
		ZkClient zkClient = null;
		//Declaration of parameter ZkUtils and initialising it to null
		ZkUtils zkUtils = null;
		try {
			//Creation of ZkClient object and initialising it by using loadProperties file
			zkClient = new ZkClient(loadProperties.getKafkaProperties().getProperty("ZOOKEEPERHOSTS"), Integer.parseInt(loadProperties.getKafkaProperties().getProperty("SESSIONTIMEOUTINMS")), Integer.parseInt(loadProperties.getKafkaProperties().getProperty("CONNECTIONTIMEOUTINMS")));
			zkClient.setZkSerializer(new ZkSerializer() {
				@Override
				public byte[] serialize(Object object) throws ZkMarshallingError {
					return ZKStringSerializer.serialize(object);
				}
				@Override
				public Object deserialize(byte[] bytes) throws ZkMarshallingError {
					return ZKStringSerializer.deserialize(bytes);
				}
			});
			//Creation of ZkUtils object and initialising it by using loadProperties file
			zkUtils = new ZkUtils(zkClient, new ZkConnection(loadProperties.getKafkaProperties().getProperty("ZOOKEEPERHOSTS")), false);
			//Creation of Properties object
			Properties topicConfiguration = new Properties();
			//Creation of topic dynamically by using AdminUtils
			AdminUtils.createTopic(zkUtils, topicName, numberOfPartitions, numberOfReplications, topicConfiguration,
					RackAwareMode.Enforced$.MODULE$);
			System.out.println(AdminUtils.topicExists(zkUtils, topicName));
		}
		//catching the exception for NoAvailableBrokersException
		catch (NoAvailableBrokersException noAvailableBrokersException) {
			noAvailableBrokersException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoAvailableBrokersException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for ZkTimeoutException
		catch (ZkTimeoutException zkTimeoutException) {
			zkTimeoutException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for ZkTimeoutException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}  
		//catching the exception for Exception
		catch (Exception ex) {
			ex.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for Exception
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		} finally {
			//if loop to check the condition zkClient not equals to null
			if (zkClient != null) {
				//closing zkClient
				zkClient.close();
			}
		}
	}
}