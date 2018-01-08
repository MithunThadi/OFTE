package com.ofte.kafka.services;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.ofte.file.services.LoadProperties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.KafkaException;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
/**
 * 
 * Class Functionality: The main functionality of this class is creation of
 * kafka topics by using zookeeper hosts
 * 
 * Methods: public void createTopic(String topicName, int numberOfPartitions,
 * int numberOfReplications)
 *
 */
public class KafkaSecondLayerConnectService {
	// Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for KafkaSecondLayerConnectService class
	Logger logger = Logger
			.getLogger(KafkaSecondLayerConnectService.class.getName());
	// Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	String hostIP;
	public KafkaSecondLayerConnectService() {
		try {
			InetAddress host = InetAddress.getLocalHost();
			hostIP = host.getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * This method is used to create the topic
	 * 
	 * @param topicName
	 * @param numberOfPartitions
	 * @param numberOfReplications
	 */
	public void createTopic(String topicName, int numberOfPartitions,
			int numberOfReplications) {
		//// Declaration of parameter ZkClient
		ZkClient zkClient = null;
		// Declaration of parameter ZkUtils
		ZkUtils zkUtils = null;
		try {
			// Creation of ZkClient object and initialising it by using
			// loadProperties file
			zkClient = new ZkClient(
					hostIP + loadProperties.getSecondLayerProperties()
							.getProperty("ZOOKEEPERHOSTS"),
					Integer.parseInt(loadProperties.getSecondLayerProperties()
							.getProperty("SESSIONTIMEOUTINMS")),
					Integer.parseInt(loadProperties.getSecondLayerProperties()
							.getProperty("CONNECTIONTIMEOUTINMS")));
			zkClient.setZkSerializer(new ZkSerializer() {
				@Override
				public byte[] serialize(Object object)
						throws ZkMarshallingError {
					return ZKStringSerializer.serialize(object);
				}
				@Override
				public Object deserialize(byte[] bytes)
						throws ZkMarshallingError {
					return ZKStringSerializer.deserialize(bytes);
				}
			});
			// Creation of ZkUtils object and initialising it by using
			// loadProperties file
			zkUtils = new ZkUtils(zkClient,
					new ZkConnection(
							hostIP + loadProperties.getSecondLayerProperties()
									.getProperty("ZOOKEEPERHOSTS")),
					false);
			// Creation of Properties object
			Properties topicConfiguration = new Properties();
			// Cfreation of topic
			AdminUtils.createTopic(zkUtils, topicName, numberOfPartitions,
					numberOfReplications, topicConfiguration,
					RackAwareMode.Enforced$.MODULE$);
			System.out.println(AdminUtils.topicExists(zkUtils, topicName));
		}
		// catching the exception for NoAvailableBrokersException
		catch (NoAvailableBrokersException noAvailableBrokersException) {
			noAvailableBrokersException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoAvailableBrokersException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for ZkTimeoutException
		catch (ZkTimeoutException zkTimeoutException) {
			zkTimeoutException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for ZkTimeoutException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for KafkaException
		catch (KafkaException kafkaException) {
			kafkaException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for org.apache.kafka.common.KafkaException
		catch (org.apache.kafka.common.KafkaException commonskafkaException) {
			commonskafkaException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for org.apache.kafka.common.KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for ZkException
		catch (ZkException zkException) {
			zkException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for ZkException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		} finally {
			// if loop to check the condition zkClient not equals to null
			if (zkClient != null) {
				// closing zkClient
				zkClient.close();
			}
		}
	}
}