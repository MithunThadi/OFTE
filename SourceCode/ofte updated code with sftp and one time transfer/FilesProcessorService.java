package com.sftp.services;

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
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ofte.services.Acknowledgement;
import com.ofte.services.KafkaConnectService;

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
 * Class Functionality:
 * 						The main functionality of this class is depending upon the part size it is splitting the file into number of parts and publishing data into kafkaserver and consuming the data and also parallelly updating the database
 * Methods:
 * 			public void publish(String TOPIC, String Key, String Message, Map<String, String> metadata,Map<String, String> transferMetaData)
 *			public void consume(String TOPIC, Map<String, String> metadata, Session session,Map<String, String> transferMetaData)
 *			public void getMessages(String sourceFile, Map<String, String> metadata, Map<String, String> transferMetaData)
 */
@SuppressWarnings("deprecation")
public class FilesProcessorService {
	//Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	//Creating Logger object for FilesProcessorService class
	 Logger logger = Logger.getLogger(FilesProcessorService.class.getName());
	//Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	//Creation of ZkClient object and initialising it with loadProperties file
	 ZkClient zkClient = new ZkClient(loadProperties.getKafkaProperties().getProperty("ZOOKEEPER.CONNECT"), Integer.parseInt(loadProperties.getKafkaProperties().getProperty("SESSIONTIMEOUT")), Integer.parseInt(loadProperties.getKafkaProperties().getProperty("CONNECTIONTIMEOUT")), ZKStringSerializer$.MODULE$);
	 //Declaration of parameter ConsumerConnector and initialising it to null
	 ConsumerConnector consumerConnector = null;
	//Declaration of parameter publishCount and initialising it to zero
	public int publishCount = 0;
	//Declaration of parameter subscribeCount and initialising it to zero
	public int subscribeCount = 0;
	//Creating an object for CassandraInteracter class
	CassandraInteracter cassandraInteracter=new CassandraInteracter();
	/**
	 * This method is used to publish the data
	 * @param TOPIC
	 * @param Key
	 * @param Message
	 * @param metadata
	 * @param transferMetaData
	 */
	public void publish(String TOPIC, String Key, String Message, Map<String, String> metadata,
			Map<String, String> transferMetaData) {
		try {
			System.out.println("setting zkclient");
			//Creation of ZkUtils object and initialising it with loadProperties file
			ZkUtils zkutils = new ZkUtils(zkClient, new ZkConnection(loadProperties.getKafkaProperties().getProperty("ZOOKEEPER.CONNECT")), true);
			//if loop to check the condition topicExists or not
			if (!AdminUtils.topicExists(zkutils, TOPIC)) {
				//Creating an object for KafkaConnectService class
				KafkaConnectService kafkaConnectService=new KafkaConnectService();
				//Creation of topic 
				kafkaConnectService.createTopic(TOPIC, Integer.parseInt(loadProperties.getKafkaProperties().getProperty("NUMBEROFPARTITIONS")), Integer.parseInt(loadProperties.getKafkaProperties().getProperty("NUMBEROFREPLICATIONS")));
			}
			System.out.println("created success");
			//Creation of Properties object
			Properties properties = new Properties();
			properties.put("metadata.broker.list",loadProperties.getKafkaProperties().getProperty("METADATA.BROKER.LIST") );
			properties.put("serializer.class", new String(loadProperties.getKafkaProperties().getProperty("SERIALIZER.CLASS")));
			properties.put("reconnect.backoff.ms",(String) loadProperties.getKafkaProperties().getProperty("RECONNECT.BACKOFF.MS"));
			properties.put("retry.backoff.ms",(String) loadProperties.getKafkaProperties().getProperty("RETRY.BACKOFF.MS"));
			properties.put("producer.type",(String) loadProperties.getKafkaProperties().getProperty("PRODUCER.TYPE"));
			properties.put("message.send.max.retries",(String) loadProperties.getKafkaProperties().getProperty("MESSAGE.SEND.MAX.RETRIES"));
			properties.put("message.max.bytes",(String) loadProperties.getKafkaProperties().getProperty("MESSAGE.MAX.BYTES"));
			//Creation of ProducerConfig object 
			ProducerConfig producerConfig = new ProducerConfig(properties);
			//Creation of Producer object 
			kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
					producerConfig);
			//Creation of KeyedMessage object 
			KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, Key, Message);
			//Sending the messages to producer
			producer.send(message);
			//Inserting publishCount to transferMetaData
			transferMetaData.put("incrementPublish", Integer.toString(publishCount++));
			//Updating the database
			cassandraInteracter.updateTransferEventPublishDetails(cassandraInteracter.connectCassandra(), transferMetaData);
			//closing th producer
			producer.close();
			//Invoking the consume method
			consume(TOPIC, metadata, cassandraInteracter.connectCassandra(), transferMetaData);
			System.out.println("Consumed Successfully: " + TOPIC);
			////Updating the database
			cassandraInteracter.updateTransferDetails(cassandraInteracter.connectCassandra(), transferMetaData, metadata);
			//Creating an object for KafkaSecondLayer class
			KafkaSecondLayer kafkaSecondLayer=new KafkaSecondLayer();
			//publishing the monitor_transfer table data
			kafkaSecondLayer.publish(loadProperties.getOFTEProperties().getProperty("TOPICNAME1"), transferMetaData.get("transferId"),
					cassandraInteracter.kafkaSecondCheckTransfer(cassandraInteracter.connectCassandra(),
							transferMetaData.get("transferId")));
			System.out.println("updated cass: " + TOPIC);
			System.out.println("unlocking");
		} 
		//catching the exception for KafkaException
		catch (KafkaException kafkaException) {
			kafkaException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for KafkaStorageException
		catch (KafkaStorageException kafkaStorageException) {
			kafkaStorageException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for KafkaStorageException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for ZkException
		catch (ZkException zkException) {
			zkException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for ZkException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for NoAvailableBrokersException
		catch (NoAvailableBrokersException noAvailableBrokersException) {
			noAvailableBrokersException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoAvailableBrokersException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION")+ log4jStringWriter.toString());
		} 
		//catching the exception for Exception
		catch (Exception e) {
			e.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for Exception
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
	/**
	 * This method is used to consume the data
	 * @param TOPIC
	 * @param metadata
	 * @param session
	 * @param transferMetaData
	 */
	public void consume(String TOPIC, Map<String, String> metadata, Session session,
			Map<String, String> transferMetaData) {
		try {
			//Creation of Map object 
			Map<String, Integer> topicCount = new HashMap<String, Integer>();
			//Creation of Properties object 
			Properties properties = new Properties();
			properties.put("zookeeper.connect", loadProperties.getKafkaProperties().getProperty("ZOOKEEPER.CONNECT"));
			properties.put("group.id", loadProperties.getKafkaProperties().getProperty("GROUP.ID"));
			properties.put("enable.auto.commit",loadProperties.getKafkaProperties().getProperty("ENABLE.AUTO.COMMIT"));
			properties.put("auto.commit.interval.ms", loadProperties.getKafkaProperties().getProperty("AUTO.COMMIT.INTERVAL.MS"));
			properties.put("auto.offset.reset", loadProperties.getKafkaProperties().getProperty("AUTO.OFFSET.RESET"));
			properties.put("session.timeout.ms", loadProperties.getKafkaProperties().getProperty("SESSION.TIMEOUT.MS"));
			properties.put("key.deserializer", loadProperties.getKafkaProperties().getProperty("KEY.DESERIALIZER"));
			properties.put("value.deserializer", loadProperties.getKafkaProperties().getProperty("VALUE.DESERIALIZER"));
			properties.put("fetch.message.max.bytes", loadProperties.getKafkaProperties().getProperty("FETCH.MESSAGE.MAX.BYTES"));
			//Creation of ConsumerConfig object 
			ConsumerConfig conConfig = new ConsumerConfig(properties);
			//Creating the consumerConnector
			consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(conConfig);
			//Inserting the values to topicCount
			topicCount.put(TOPIC, new Integer(1));
			//Creation of Map object for consumerStreams
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
					.createMessageStreams(topicCount);
			//Creation of List for kafkaStreamList
			List<KafkaStream<byte[], byte[]>> kafkaStreamList = consumerStreams.get(TOPIC);
			//for each loop to iterate kafkaStreamList
			for (final KafkaStream<byte[], byte[]> kafkaStreams : kafkaStreamList) {
				//Getting the kafka streams
				ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStreams.iterator();
				//Inserting destinationDirectory to transferMetaData
				transferMetaData.put("destinationFile", metadata.get("destinationDirectory") + "\\" + TOPIC);
				//Declaration of parameter FileWriter
				FileWriter destinationFileWriter;
				//while loop to iterate consumerIterator
				while (consumerIterator.hasNext()) {
					try {
						//Creating an object for FileWriter class
						destinationFileWriter = new FileWriter(
								new File(metadata.get("destinationDirectory") + "\\" + TOPIC), true);
						//Writing the  kafka messages to destination file
						destinationFileWriter.write(new String(consumerIterator.next().message()));
						//closing the destinationFileWriter
						destinationFileWriter.close();
						//Inserting subscribeCount to transferMetaData
						transferMetaData.put("incrementConsumer", Integer.toString(subscribeCount++));
						//Updating the database
						cassandraInteracter.updateTransferEventConsumeDetails(session, transferMetaData);
						//shutdown the consumerConnector
						consumerConnector.shutdown();
						System.out.println("done for : " + TOPIC);
						break;
					} 
					//catching the exception for Exception
					catch (Exception e) {
						System.out.println(e);
					}
				}
				System.out.println("exited");
			}
			System.out.println("Cdone for : " + TOPIC);
			//if loop to check the condition consumerConnector not equals to null
			if (consumerConnector != null)
				consumerConnector.shutdown();
		} 
		//catching the exception for KafkaException
		catch (KafkaException kafkaException) {
			kafkaException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for KafkaException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for KafkaStorageException
		catch (KafkaStorageException kafkaStorageException) {
			kafkaStorageException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for KafkaStorageException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for ZkException
		catch (ZkException zkException) {
			zkException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for ZkException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}

		/**
		 * This method is used to split the files
		 * @param sourceFile
		 * @param metadata
		 * @param transferMetaData
		 */
	public void getMessages(String sourceFile, Map<String, String> metadata, Map<String, String> transferMetaData) {
		//Declaration of parameter delimiter
		String delimiter = "\\\\";
		//Creating an object for File class
		File inputFile = new File(sourceFile);
		//Declaration of parameter FileInputStream
		FileInputStream inputStream;
		//Declaration of parameter Key
		String Key;
		//Declaration of parameter sourceFileName and initialising it to null
		String sourceFileName = null;
		//Declaration of parameter sourceFileArray[] and splitting sourceFileDirectory using on delimiter
		String sourceFileArray[] = sourceFile.split(delimiter);
		//Declaration of parameter sourceFileArraySize and initialising it to sourceFileArray.length
		int sourceFileArraySize = sourceFileArray.length;
		sourceFileName = sourceFileArray[sourceFileArraySize - 1];
		//Declaration of parameter sourceFileSize initialising it to inputFile.length
		int sourceFileSize = (int) inputFile.length();
		System.out.println("filesize is" + sourceFileSize);
		//Declaration of parameter nChunks
		//Declaration of parameter read
		//Declaration of parameter readLength
		int nChunks = 0, read = 0, readLength = Integer.parseInt(loadProperties.getOFTEProperties().getProperty("PART_SIZE"));
		//Declaration of parameter byteChunkPart
		byte[] byteChunkPart;
		try {
			//Creating an object for FileInputStream class
			inputStream = new FileInputStream(inputFile);
			//while loop to check the sourceFileSize> 0
			while (sourceFileSize > 0) {
				//if loop to check the inputStream.available() < readLength
				if (inputStream.available() < readLength) {
					//Initialising the byte chunk part with inputStream 
					byteChunkPart = new byte[inputStream.available()];
					//Initialising the read with inputStream bytes
					read = inputStream.read(byteChunkPart, 0, inputStream.available());
				} else {
					byteChunkPart = new byte[readLength];
					read = inputStream.read(byteChunkPart, 0, readLength);
				}
				//Deducting the sourceFileSize with read size
				sourceFileSize -= read;
				assert (read == byteChunkPart.length);
				//Incrementing nChunks
				nChunks++;
				//Initialising key value
				Key = sourceFileName + "." + (nChunks - 1);
				System.out.println(sourceFileName);
				//Publishing the data
				publish(sourceFileName, Key, new String(byteChunkPart), metadata, transferMetaData);
				System.out.println("completed for thread: " + sourceFileName);

			}
			//closing inputStream
			inputStream.close();
			System.out.println("closing Stream for " + inputFile);
			//Initialising publishCount and subscribeCount with zero
			publishCount=0;
			subscribeCount=0;
			//Creating an object for Acknowledgement class
			Acknowledgement acknowledgement=new Acknowledgement();
			acknowledgement.acknowledge(transferMetaData, metadata);
		}
		//catching the exception for FileNotFoundException
		catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for FileNotFoundException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for IOException
		catch (IOException exception) {
			exception.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for IOException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
}