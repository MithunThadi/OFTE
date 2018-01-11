package com.ofte.kafka.services;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
//import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ofte.file.services.LoadProperties;

import kafka.admin.AdminUtils;
//import kafka.admin.RackAwareMode;
import kafka.common.KafkaException;
import kafka.common.KafkaStorageException;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
/**
 * 
 * Class Functionality: The main functionality of this class is publishing the
 * data to kafka server i.e., it checks whether the kafka topics existed or not,
 * if not it creates kafka topics and then consuming the data in a file
 * 
 * Methods: public void publish(String TOPIC, String Key, String Message) public
 * void consume(String TOPIC)
 *
 */
@SuppressWarnings("deprecation")
public class KafkaSecondLayer {
	// Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for KafkaSecondLayerConnectService class
	Logger logger = Logger.getLogger(KafkaSecondLayer.class.getName());
	// Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	// Creation of ZkClient object and initialising it by using loadProperties
	// file
	String hostIP;
	public KafkaSecondLayer() {
		try {
			InetAddress host = InetAddress.getLocalHost();
			hostIP = host.getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	// ZkClient zkClient = new ZkClient(
	// hostIP + loadProperties.getSecondLayerProperties()
	// .getProperty("ZOOKEEPER.CONNECT"),
	// Integer.parseInt(loadProperties.getSecondLayerProperties()
	// .getProperty("SESSIONTIMEOUT")),
	// Integer.parseInt(loadProperties.getSecondLayerProperties()
	// .getProperty("CONNECTIONTIMEOUT")),
	// ZKStringSerializer$.MODULE$);
	// Declaration of parameter ConsumerConnector and initialising it to null
	ConsumerConnector consumerConnector = null;

	/**
	 * This method publish the data into the kafka
	 * 
	 * @param TOPIC
	 * @param Key
	 * @param Message
	 * @throws InterruptedException
	 */
	public void publish(String TOPIC, String Key, String Message)
			throws InterruptedException {
		try {
			ZkClient zkClient = new ZkClient(
					hostIP + loadProperties.getSecondLayerProperties()
							.getProperty("ZOOKEEPER.CONNECT"),
					Integer.parseInt(loadProperties.getSecondLayerProperties()
							.getProperty("SESSIONTIMEOUT")),
					Integer.parseInt(loadProperties.getSecondLayerProperties()
							.getProperty("CONNECTIONTIMEOUT")),
					ZKStringSerializer$.MODULE$);
			// Creation of ZkUtils object and initialising it by using
			// loadProperties file
			ZkUtils zkutils = new ZkUtils(zkClient,
					new ZkConnection(
							hostIP + loadProperties.getSecondLayerProperties()
									.getProperty("ZOOKEEPER.CONNECT")),
					true);
			// if loop to check the condition whether the topicExists or not
			if (!AdminUtils.topicExists(zkutils, TOPIC)) {
				// Creating an object for KafkaSecondLayerConnectService class
				KafkaSecondLayerConnectService kafkaSecondLayerConnectService = new KafkaSecondLayerConnectService();
				// Creation of topic based on NUMBEROFPARTITIONS and
				// NUMBEROFREPLICATIONS
				kafkaSecondLayerConnectService.createTopic(TOPIC,
						Integer.parseInt(
								loadProperties.getSecondLayerProperties()
										.getProperty("NUMBEROFPARTITIONS")),
						Integer.parseInt(
								loadProperties.getSecondLayerProperties()
										.getProperty("NUMBEROFREPLICATIONS")));
//				Thread.currentThread().wait(1000);
			}
			// Creation of Properties object
			Properties properties = new Properties();
			properties.put("metadata.broker.list",
					hostIP + loadProperties.getSecondLayerProperties()
							.getProperty("METADATA.BROKER.LIST"));
			properties.put("serializer.class",
					loadProperties.getSecondLayerProperties()
							.getProperty("SERIALIZER.CLASS"));
			properties.put("reconnect.backoff.ms",
					loadProperties.getSecondLayerProperties()
							.getProperty("RECONNECT.BACKOFF.MS"));
			properties.put("retry.backoff.ms",
					loadProperties.getSecondLayerProperties()
							.getProperty("RETRY.BACKOFF.MS"));
			properties.put("producer.type", loadProperties
					.getSecondLayerProperties().getProperty("PRODUCER.TYPE"));
			properties.put("message.send.max.retries",
					loadProperties.getSecondLayerProperties()
							.getProperty("MESSAGE.SEND.MAX.RETRIES"));
			properties.put("message.max.bytes",
					loadProperties.getSecondLayerProperties()
							.getProperty("MESSAGE.MAX.BYTES"));
			// Creation of ProducerConfig object
			ProducerConfig producerConfig = new ProducerConfig(properties);
			// Creation of Producer object
			kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
					producerConfig);
			// Creation of KeyedMessage object
			KeyedMessage<String, String> message = new KeyedMessage<String, String>(
					TOPIC, Key, Message);
			// Publishing messages to producer
			producer.send(message);
			// Declaration of parameter Status and initialising it to published
			// successfully
			String Status = "published succesfully";
			System.out.println(Status);
			// closing producer
			producer.close();
			// Creating an object for KafkaSecondLayer class
			KafkaSecondLayer kafkaSecondLayer = new KafkaSecondLayer();
			// Invoking consume method
			kafkaSecondLayer.consume(TOPIC);
		}
		// catching the exception for KafkaException
		catch (KafkaException KafkaException) {
			KafkaException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for org.apache.kafka.common.KafkaException
		catch (org.apache.kafka.common.KafkaException commonsKafkaException) {
			commonsKafkaException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for org.apache.kafka.common.KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for KafkaStorageException
		catch (KafkaStorageException KafkaStorageException) {
			KafkaStorageException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for KafkaStorageException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for ZkException
		catch (ZkException zkException) {
			zkException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for ZkException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
	/**
	 * This method consumes the data from the kafka
	 * 
	 * @param TOPIC
	 */
	public void consume(String TOPIC) {
		try {
			// Creation of Properties object
			Properties properties = new Properties();
			properties.put("zookeeper.connect",
					hostIP + loadProperties.getSecondLayerProperties()
							.getProperty("ZOOKEEPER.CONNECT"));
			properties.put("group.id", loadProperties.getSecondLayerProperties()
					.getProperty("GROUP.ID"));
			properties.put("enable.auto.commit",
					loadProperties.getSecondLayerProperties()
							.getProperty("ENABLE.AUTO.COMMIT"));
			properties.put("auto.commit.interval.ms",
					loadProperties.getSecondLayerProperties()
							.getProperty("AUTO.COMMIT.INTERVAL.MS"));
			properties.put("auto.offset.reset",
					loadProperties.getSecondLayerProperties()
							.getProperty("AUTO.OFFSET.RESET"));
			properties.put("session.timeout.ms",
					loadProperties.getSecondLayerProperties()
							.getProperty("SESSION.TIMEOUT.MS"));
			properties.put("key.deserializer",
					loadProperties.getSecondLayerProperties()
							.getProperty("KEY.DESERIALIZER"));
			properties.put("value.deserializer",
					loadProperties.getSecondLayerProperties()
							.getProperty("VALUE.DESERIALIZER"));
			properties.put("fetch.message.max.bytes",
					loadProperties.getSecondLayerProperties()
							.getProperty("FETCH.MESSAGE.MAX.BYTES"));
			// Creation of ConsumerConfig object
			ConsumerConfig conConfig = new ConsumerConfig(properties);
			// Creating the consumerConnector
			consumerConnector = kafka.consumer.Consumer
					.createJavaConsumerConnector(conConfig);
			// Creation of Map object
			Map<String, Integer> topicCount = new HashMap<String, Integer>();
			// Inserting the values to topicCount
			topicCount.put(TOPIC, new Integer(1));
			// Creation of Map object for consumerStreams
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
					.createMessageStreams(topicCount);
			// Creation of List for kafkaStreamList
			List<KafkaStream<byte[], byte[]>> kafkaStreamList = consumerStreams
					.get(TOPIC);
			// for each loop to iterate kafkaStreamList
			for (final KafkaStream<byte[], byte[]> kafkaStreams : kafkaStreamList) {
				ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStreams
						.iterator();
				File file = null;
				// Creating an object for File class
				if (System.getProperty("os.name").contains("Linux")) {
					file = new File(loadProperties.getSecondLayerProperties()
							.getProperty("SECONDLAYERFILENAMELINUX"));
				} else if (System.getProperty("os.name").contains("Windows")) {
					file = new File(loadProperties.getSecondLayerProperties()
							.getProperty("SECONDLAYERFILENAMEWINDOWS"));
				}
				// Declaration of parameter FileWriter
				FileWriter fileWriter;
				// while loop to iterate consumerIterator
				while (consumerIterator.hasNext()) {
					try {
						// Creating an object for FileWriter class
						fileWriter = new FileWriter(file, true);
						// Writing the kafka messages to destination file
						fileWriter.write(
								new String(consumerIterator.next().message()));
						// closing fileWriter
						fileWriter.close();
						// shutting down consumerConnector
						consumerConnector.shutdown();
					} catch (Exception e) {
						System.out.println(e);
					}
				}
			}
			// if loop to check the condition consumerConnector not equals to
			// null
			if (consumerConnector != null)
				consumerConnector.shutdown();
		}
		// catching the exception for KafkaException
		catch (KafkaException KafkaException) {
			KafkaException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for org.apache.kafka.common.KafkaException
		catch (org.apache.kafka.common.KafkaException commonsKafkaException) {
			commonsKafkaException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for org.apache.kafka.common.KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for KafkaStorageException
		catch (KafkaStorageException KafkaStorageException) {
			KafkaStorageException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for KafkaStorageException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for ZkException
		catch (ZkException zkException) {
			zkException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for ZkException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
}