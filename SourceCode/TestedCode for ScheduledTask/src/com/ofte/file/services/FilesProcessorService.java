package com.ofte.file.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ofte.cassandra.services.CassandraInteracter;
import com.ofte.kafka.services.KafkaSecondLayer;
import com.ofte.kafka.services.KafkaServerService;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.FailedToSendMessageException;
//import kafka.admin.RackAwareMode;
import kafka.common.KafkaException;
import kafka.common.KafkaStorageException;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
/**
 * 
 * Class Functionality: The main functionality of this class is depending upon
 * the part size it is splitting the file into number of parts and publishing
 * data into kafkaserver and consuming the data and also parallelly updating the
 * database Methods: public void publish(String TOPIC, String Key, String
 * Message, Map<String, String> metadata,Map<String, String> transferMetaData)
 * public void consume(String TOPIC, Map<String, String> metadata, Session
 * session,Map<String, String> transferMetaData) public void getMessages(String
 * sourceFile, Map<String, String> metadata, Map<String, String>
 * transferMetaData)
 */
@SuppressWarnings("deprecation")
public class FilesProcessorService {
	// Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for FilesProcessorService class
	Logger logger = Logger.getLogger(FilesProcessorService.class.getName());
	// Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	// Creation of ZkClient object and initialising it with loadProperties file
	// ZkClient zkClient = new
	// ZkClient(loadProperties.getKafkaProperties().getProperty("ZOOKEEPER.CONNECT"),
	// Integer.parseInt(loadProperties.getKafkaProperties().getProperty("SESSIONTIMEOUT")),
	// Integer.parseInt(loadProperties.getKafkaProperties().getProperty("CONNECTIONTIMEOUT")),
	// ZKStringSerializer$.MODULE$);
	// Declaration of parameter ConsumerConnector and initialising it to null
	ConsumerConnector consumerConnector = null;
	// Declaration of parameter publishCount and initialising it to zero
	public int publishCount = 0;
	// Declaration of parameter subscribeCount and initialising it to zero
	public int subscribeCount = 0;

	// Declaration of parameter FileWriter
	

	// Creating an object for CassandraInteracter class
	CassandraInteracter cassandraInteracter = new CassandraInteracter();
	KafkaServerService kafkaServerService = new KafkaServerService();
	UserExitPoints userExitPoints = new UserExitPoints();

	/**
	 * This method is used to publish the data
	 * 
	 * @param TOPIC
	 * @param Key
	 * @param Message
	 * @param metadata
	 * @param transferMetaData
	 * @throws IOException 
	 * 
	 * 
	 */
	// String brokerPort = kafkaServerService.getBrokerAddress();
	// String zookeeperPort = kafkaServerService.getZKAddress();
	// String groupId = kafkaServerService.getId();
	public void publish(String TOPIC, String Key, String Message,
			ZkUtils zkutils, ZkClient zkClient, Map<String, String> metadata,
			Map<String, String> transferMetaData) throws IOException {
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		KafkaServerService kafkaServerService = new KafkaServerService();
		UserExitPoints userExitPoints = new UserExitPoints();
		//
		
		try {
			System.out.println("setting zkclient");
			System.out.println(AdminUtils.topicExists(zkutils, TOPIC));
			if (zkClient != null) {
				System.out.println("ZKCLIENT");
			}
			if (zkutils != null) {
				System.out.println("ZKUTILS");
			}
			if (!AdminUtils.topicExists(zkutils, TOPIC)) {
				// Creating an object for KafkaConnectService class
				System.out.println("Entered into if loop");
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
				System.out.println("Running in the if loop");
				Properties topicConfiguration = new Properties();
				AdminUtils.createTopic(zkutils, TOPIC, 1, 1, topicConfiguration,
						RackAwareMode.Enforced$.MODULE$);

				System.out.println("after creation of topic");
				// kafkaConnectService.createTopic(TOPIC,zkClient,zkutils,
				// Integer.parseInt(loadProperties.getKafkaProperties().getProperty("NUMBEROFPARTITIONS")),
				// Integer.parseInt(loadProperties.getKafkaProperties().getProperty("NUMBEROFREPLICATIONS")));
			}
			System.out.println("created success");
			ProducerConfig producerConfig = kafkaServerService
					.getProducerConfig();
			// Creation of Producer object
			kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
					producerConfig);
			// Creation of KeyedMessage object
			KeyedMessage<String, String> message = new KeyedMessage<String, String>(
					TOPIC, Key, Message);
			// Sending the messages to producer
			try {
			producer.send(message);
			}
			catch(FailedToSendMessageException e) {
				System.out.println("entered............................................failed to send");
				publish(TOPIC+"1", Key, Message, zkutils, zkClient, metadata, transferMetaData);
				System.out.println("closed...........................................");
			}
			transferMetaData.put("incrementPublish",
					Integer.toString(publishCount++));
			// Updating the database
			cassandraInteracter.updateTransferEventPublishDetails(
					cassandraInteracter.connectCassandra(), transferMetaData);
			// closing th producer
			producer.close();

			// metadata.put("preDestination", "ADD|1|2~SUB|4|1|1");
			// preDestination Condition
			if (metadata.get("preDestination") != null) {
				String preDestination = metadata.get("preDestination");
				int result = userExitPoints.accessExitPoint(preDestination,
						metadata, transferMetaData);
				System.out.println(result);
			}

			System.out.println(TOPIC + " " + metadata + " " + transferMetaData);
			// Invoking the consume method
			consume(TOPIC, metadata, cassandraInteracter.connectCassandra(),
					transferMetaData);
			System.out.println("Consumed Successfully: " + TOPIC);

			// metadata.put("postDestination", "ADD|1|2~SUB|4|1|1");
			// postDestination Condition
			if (metadata.get("postDestination") != null) {
				String postDestination = metadata.get("postDestination");
				int result = userExitPoints.accessExitPoint(postDestination,
						metadata, transferMetaData);
				System.out.println(result);
			}

			//// Updating the database
			// cassandraInteracter.updateTransferDetails(
			// cassandraInteracter.connectCassandra(), transferMetaData,
			// metadata);
			// Creating an object for KafkaSecondLayer class
			KafkaSecondLayer kafkaSecondLayer = new KafkaSecondLayer();
			// publishing the monitor_transfer table data
			try {
				kafkaSecondLayer.publish(
						loadProperties.getOFTEProperties()
								.getProperty("TOPICNAME1"),
						transferMetaData.get("transferId"),
						cassandraInteracter.kafkaSecondCheckTransfer(
								cassandraInteracter.connectCassandra(),
								transferMetaData.get("transferId")));
			} catch (NoSuchFieldException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("updated cass: " + TOPIC);
			System.out.println("unlocking");
		}
		// catching the exception for KafkaException
		catch (KafkaException kafkaException) {
			kafkaException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for KafkaStorageException
		catch (KafkaStorageException kafkaStorageException) {
			kafkaStorageException
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
		// catching the exception for NoAvailableBrokersException
		catch (NoAvailableBrokersException noAvailableBrokersException) {
			noAvailableBrokersException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoAvailableBrokersException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		
	}
	/**
	 * This method is used to consume the data
	 * 
	 * @param TOPIC
	 * @param metadata
	 * @param session
	 * @param transferMetaData
	 * @throws IOException 
	 */
	public void consume(String TOPIC, Map<String, String> metadata,
			Session session, Map<String, String> transferMetaData) throws IOException {
		try {
			// Creation of Map object
			Map<String, Integer> topicCount = new HashMap<String, Integer>();
			CassandraInteracter cassandraInteracter = new CassandraInteracter();
			KafkaServerService kafkaServerService = new KafkaServerService();
			ConsumerConfig conConfig = kafkaServerService.getConsumerConfig();
			consumerConnector = kafka.consumer.Consumer
					.createJavaConsumerConnector(conConfig);
			FileWriter destinationFileWriter = null;
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
				// Getting the kafka streams
				ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStreams
						.iterator();
				// Inserting destinationDirectory to transferMetaData
				// transferMetaData.put("destinationFile",
				// metadata.get("destinationDirectory") + "\\" + TOPIC);
				// // Declaration of parameter FileWriter
				// FileWriter destinationFileWriter;
				// while loop to iterate consumerIterator
				while (consumerIterator.hasNext()) {
					try {
						File createDirectory = new File(
								metadata.get("destinationDirectory"));
						if (!createDirectory.exists()) {

							FileUtils.forceMkdir(createDirectory);
						} else {
							System.out.println("failed to create directory");
						}
						// Creating an object for FileWriter class
						destinationFileWriter = new FileWriter(
								new File(transferMetaData.get("destinationFile").replace("\r", "")),
								true);
						// Writing the kafka messages to destination file
						destinationFileWriter.write(
								new String(consumerIterator.next().message()));
						// closing the destinationFileWriter
//						destinationFileWriter.close();
						// subscribeCount = subscribeCount++;
						// Inserting subscribeCount to transferMetaData
						transferMetaData.put("incrementConsumer",
								Integer.toString(subscribeCount++));
						// Updating the database
						cassandraInteracter.updateTransferEventConsumeDetails(
								session, transferMetaData);
						// shutdown the consumerConnector
						consumerConnector.shutdown();
						System.out.println("done for : " + TOPIC);
						break;
					}
					// catching the exception for Exception
					catch (Exception e) {
						System.out.println(e);
					}finally {
						if (destinationFileWriter != null) {
						destinationFileWriter.close();
						}
					}
				}
				System.out.println("exited");
			}
			System.out.println("Cdone for : " + TOPIC);
			// if loop to check the condition consumerConnector not equals to
			// null
			if (consumerConnector != null)
				consumerConnector.shutdown();
		}
		// catching the exception for KafkaException
		catch (KafkaException kafkaException) {
			kafkaException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for KafkaStorageException
		catch (KafkaStorageException kafkaStorageException) {
			kafkaStorageException
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
	 * This method is used to split the files
	 * 
	 * @param zkUtils
	 * @param zkClient
	 * @param sourceFile
	 * @param metadata
	 * @param transferMetaData
	 * @throws IOException 
	 * 
	 */
	public void getMessages(String sourceFile, ZkClient zkClient,
			ZkUtils zkUtils, Map<String, String> metadata,
			Map<String, String> transferMetaData) throws IOException {
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		UserExitPoints userExitPoints = new UserExitPoints();
		// Declaration of parameter delimiter
		String delimiter = null;
		if (System.getProperty("os.name").contains("Linux")) {
			delimiter = "/";
		} else if (System.getProperty("os.name").contains("Windows")) {
			delimiter = "\\\\";
		}
		// Creating an object for File class
		File inputFile = new File(sourceFile);
		// Declaration of parameter FileInputStream
		FileInputStream inputStream = null;
		// Declaration of parameter Key
		String Key;
		// Declaration of parameter sourceFileName and initialising it to null
		String sourceFileName = null;
		// Declaration of parameter sourceFileArray[] and splitting
		// sourceFileDirectory using on delimiter
		String sourceFileArray[] = sourceFile.split(delimiter);
		// Declaration of parameter sourceFileArraySize and initialising it to
		// sourceFileArray.length
		int sourceFileArraySize = sourceFileArray.length;
		sourceFileName = sourceFileArray[sourceFileArraySize - 1];
		// Declaration of parameter sourceFileSize initialising it to
		// inputFile.length
		long sourceFileSize = inputFile.length();
		System.out.println("filesize is" + sourceFileSize);
		transferMetaData.put("sourceFileSize", String.valueOf(sourceFileSize));
		// Declaration of parameters to operate n files
		int nChunks = 0, read = 0;
		Long readLength = Long.parseLong(
				loadProperties.getOFTEProperties().getProperty("PART_SIZE"));
		// Declaration of parameter byteChunkPart
		byte[] byteChunkPart;
		try {
			// Creating an object for FileInputStream class
			inputStream = new FileInputStream(inputFile);
			// while loop to check the sourceFileSize> 0
			while (sourceFileSize > 0) {
				// if loop to check the inputStream.available() < readLength
				if (inputStream.available() < readLength) {
					System.out
							.println(inputStream.available() + " in if block");
					// Initialising the byte chunk part with inputStream
					byteChunkPart = new byte[inputStream.available()];
					// Initialising the read with inputStream bytes
					read = inputStream.read(byteChunkPart, 0,
							inputStream.available());
				} else {
					System.out.println(
							inputStream.available() + " in else block");
					// byteChunkPart = new byte[readLength];
					// byteChunkPart = Longs.toByteArray(readLength);
					byteChunkPart = new byte[readLength.intValue()];
					read = inputStream.read(byteChunkPart, 0,
							readLength.intValue());
				}
				// Deducting the sourceFileSize with read size
				sourceFileSize -= read;

				// Incrementing nChunks
				nChunks++;
				// Initialising key value
				Key = sourceFileName + "." + (nChunks - 1);
				System.out.println(sourceFileName);
				// Publishing the data
				publish(sourceFileName.replaceAll(" ", ""), Key, new String(byteChunkPart), zkUtils,
						zkClient, metadata, transferMetaData);
				System.out.println("completed for thread: " + sourceFileName);
				cassandraInteracter.updateTransferDetails(
						cassandraInteracter.connectCassandra(),
						transferMetaData, metadata);
				File destinationFile = new File(
						transferMetaData.get("destinationFile"));
				System.out.println("SourceFile size is "
						+ transferMetaData.get("sourceFileSize").toString());
				System.out.println("destination file size "
						+ String.valueOf(destinationFile.length()));

				if (Long.parseLong(transferMetaData.get("sourceFileSize")
						.toString()) == Long.parseLong(
								String.valueOf(destinationFile.length()))) {
					cassandraInteracter.success(
							cassandraInteracter.connectCassandra(),
							transferMetaData);
				}

			}
			// closing inputStream
			System.out.println("closing Stream for " + inputFile);
			// Initialising publishCount and subscribeCount with zero
			publishCount = 0;
			subscribeCount = 0;
			// Creating an object for Acknowledgement class
			Acknowledgement acknowledgement = new Acknowledgement();
			acknowledgement.acknowledge(transferMetaData, metadata);
			// postSource Condition
			if (metadata.get("postSource") != null) {
				String postSource = metadata.get("postSource");
				int result = userExitPoints.accessExitPoint(postSource,
						metadata, transferMetaData);
				System.out.println(result);
			}
		}
		// catching the exception for FileNotFoundException
		catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for FileNotFoundException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for IOException
		catch (IOException exception) {
			exception.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for IOException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}finally {
			if(inputStream!=null) {
				inputStream.close();
				
			}
		}
	}
}