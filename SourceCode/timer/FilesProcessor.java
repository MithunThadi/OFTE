package commm;

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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import commm.Acknowledge;
import commm.kafkaconnect;
//import cos.DBOperations;
//import cos.KafkaSecondLayer;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
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

public class FilesProcessor {
	static Logger logger = Logger.getLogger(FilesProcessor.class.getName());
	static String loggerException="Caught exception; decorating with appropriate status template : ";
	private int PART_SIZE = 16 * 1024 * 1024;
	static ZkClient zkClient = new ZkClient("172.25.26.157:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
	static ConsumerConnector consumerConnector = null;
	// private final static byte PART_SIZE = 120;
	public int publishCount = 0;
	public int subscribeCount = 0;
	public String topicName = "monitor_transfer";

	public void publish(String TOPIC, String Key, String Message, Map<String, String> metadata,
			Map<String, String> transferMetaData) {
		try {
//			long timestamp = System.currentTimeMillis();
			System.out.println("setting zkclient");
			ZkUtils zkutils = new ZkUtils(zkClient, new ZkConnection("172.25.26.157:2181"), true);
			if (!AdminUtils.topicExists(zkutils, TOPIC)) {
				kafkaconnect.createTopic(TOPIC, 1, 1);
			}
			System.out.println("created success");
//			String publishStatus = "published succesfully";
			Properties properties = new Properties();
			properties.put("metadata.broker.list", "localhost:9092");
			properties.put("serializer.class", "kafka.serializer.StringEncoder");
			properties.put("reconnect.backoff.ms", "50");
			properties.put("retry.backoff.ms", "100");
			properties.put("producer.type", "async");
			properties.put("message.send.max.retries", "2");
			// ppts.put("message.send.max.retries", "3");
			// ppts.put("log.retention.minutes", 2);
			// ppts.put("auto.offset.reset", "smallest");
			properties.put("message.max.bytes", "1073741824");
			// replica.fetch.max.bytes=15728640
			// ppts.put("replica.fetch.max.bytes", "1073741824");
			ProducerConfig producerConfig = new ProducerConfig(properties);
			kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
					producerConfig);
			KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, Key, Message);
			producer.send(message);
//			publishCount++;
//			String incrementPublish = Integer.toString(publishCount);
			// data base code to update publishCount in transfer_event
			transferMetaData.put("incrementPublish", Integer.toString(publishCount++));
			Session session = DBOperations.connectCassandra();
			DBOperations.updateTransferEventPublishDetails(session, transferMetaData);
			producer.close();
//			Lock lock = new ReentrantLock();
//			lock.lock();
			consume(TOPIC, metadata, session, transferMetaData);
			System.out.println("Consumed Successfully: " + TOPIC);
			DBOperations.updateTransferDetails(session, transferMetaData, metadata);
			KafkaSecondLayer kafkaSecondLayer=new KafkaSecondLayer();
			kafkaSecondLayer.publish(topicName, transferMetaData.get("transferId"),
					DBOperations.kafkaSecondCheckTransfer(session,
							transferMetaData.get("transferId")));
			
			System.out.println("updated cass: " + TOPIC);
			session.close();
			session.closeAsync();
//			lock.unlock();
			System.out.println("unlocking");
		} catch (KafkaException kafkaException) {
			StringWriter stack = new StringWriter();
			kafkaException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (KafkaStorageException kafkaStorageException) {
			StringWriter stack = new StringWriter();
			kafkaStorageException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (ZkException zkException) {
			StringWriter stack = new StringWriter();
			zkException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoHostAvailableException noHostAvailableException) {
			StringWriter stack = new StringWriter();
			noHostAvailableException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoAvailableBrokersException noAvailableBrokersException) {
			StringWriter stack = new StringWriter();
			noAvailableBrokersException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException+ stack.toString());
		} catch (Exception e) {
			StringWriter stack = new StringWriter();
			e.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}
	}

	public void consume(String TOPIC, Map<String, String> metadata, Session session,
			Map<String, String> transferMetaData) {
		try {
			Map<String, Integer> topicCount = new HashMap<String, Integer>();
			Properties properties = new Properties();
			properties.put("zookeeper.connect", "172.25.26.157:2181");
			properties.put("group.id", "testgroup");
			// props.put("bootstrap.servers", "localhost:9093");
			// props.put("group.id", "testgroup");
			properties.put("enable.auto.commit", "true");
			properties.put("auto.commit.interval.ms", "1000");
			properties.put("auto.offset.reset", "smallest");
			properties.put("session.timeout.ms", "30000");
			properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			// props.put("auto.offset.reset", "smallest");
			properties.put("fetch.message.max.bytes", "1073741824");
			// props.put("message.send.max.retries", "2");
			// props.put("fetch.message.max.bytes", "52428800");
			ConsumerConfig conConfig = new ConsumerConfig(properties);

			// consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
			consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(conConfig);

			topicCount.put(TOPIC, new Integer(1));
			// ConsumerConnector creates the message stream for each topic
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
					.createMessageStreams(topicCount);
			// Get Kafka stream for topic 'mytopic'`
			List<KafkaStream<byte[], byte[]>> kafkaStreamList = consumerStreams.get(TOPIC);
			for (final KafkaStream<byte[], byte[]> kafkaStreams : kafkaStreamList) {
				ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStreams.iterator();
				transferMetaData.put("destinationFile", metadata.get("destinationDirectory") + "\\" + TOPIC);
				FileWriter destinationFileWriter;
				while (consumerIterator.hasNext()) {
					try {
						destinationFileWriter = new FileWriter(
								new File(metadata.get("destinationDirectory") + "\\" + TOPIC), true);
						destinationFileWriter.write(new String(consumerIterator.next().message()));
						destinationFileWriter.close();
						transferMetaData.put("incrementConsumer", Integer.toString(subscribeCount++));
						DBOperations.updateTransferEventConsumeDetails(session, transferMetaData);
						consumerConnector.shutdown();
						System.out.println("done for : " + TOPIC);
						break;
					} catch (Exception e) {
						System.out.println(e);
					}
				}
				System.out.println("exited");
			}
			System.out.println("Cdone for : " + TOPIC);
			// Shutdown the consumer connector
			if (consumerConnector != null)
				consumerConnector.shutdown();
		} catch (KafkaException kafkaException) {
			StringWriter stack = new StringWriter();
			kafkaException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (KafkaStorageException kafkaStorageException) {
			StringWriter stack = new StringWriter();
			kafkaStorageException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (ZkException zkException) {
			StringWriter stack = new StringWriter();
			zkException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoHostAvailableException noHostAvailableException) {
			StringWriter stack = new StringWriter();
			noHostAvailableException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}
	}

	public String createTopic(ZkUtils zkutils, String topicName) {
		ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
		zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
		Properties topicConfig = new Properties();
		AdminUtils.createTopic(zkutils, topicName, 1, 3, topicConfig, RackAwareMode.Enforced$.MODULE$);
		zkClient.close();

		return topicName;
	}

	public String deleteTopic(String topicName) {
		String deleteTopicStatus = topicName + " : topic is deleted successfully";
		ZkClient zkClient = new ZkClient("172.25.26.157:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
		zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
		return deleteTopicStatus;
	}

	public void getMessages(String sourceFile, Map<String, String> metadata, Map<String, String> transferMetaData) {

		String delimiter = "\\\\";
		File inputFile = new File(sourceFile);
		FileInputStream inputStream;
//		String spath2 = sourceFile.toString();
		String Key;
		String sourceFileName = null;
		String sourceFileArray[] = sourceFile.split(delimiter);
		int sourceFileArraySize = sourceFileArray.length;
		sourceFileName = sourceFileArray[sourceFileArraySize - 1];
		int sourceFileSize = (int) inputFile.length();
		System.out.println("filesize is" + sourceFileSize);
		int nChunks = 0, read = 0, readLength = PART_SIZE;
		byte[] byteChunkPart;
		try {
			inputStream = new FileInputStream(inputFile);
			while (sourceFileSize > 0) {

				if (inputStream.available() < readLength) {
					byteChunkPart = new byte[inputStream.available()];
					// IOUtils.copyLarge();
					read = inputStream.read(byteChunkPart, 0, inputStream.available());
				} else {
					byteChunkPart = new byte[readLength];
					read = inputStream.read(byteChunkPart, 0, readLength);
				}
				sourceFileSize -= read;
				assert (read == byteChunkPart.length);
				nChunks++;
				Key = sourceFileName + "." + (nChunks - 1);
				System.out.println(sourceFileName);
//				Lock lock = new ReentrantLock();
//				lock.lock();
				publish(sourceFileName, Key, new String(byteChunkPart), metadata, transferMetaData);
//				lock.unlock();
				System.out.println("completed for thread: " + sourceFileName);

			}
			inputStream.close();
			System.out.println("closing Stream for " + inputFile);
			publishCount=0;
			subscribeCount=0;
			Acknowledge.acknowledge(transferMetaData, metadata);
			// To Do code to update and insert into DB
		} catch (FileNotFoundException fileNotFoundException) {
			StringWriter stack = new StringWriter();
			fileNotFoundException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}catch (IOException exception) {
			StringWriter stack = new StringWriter();
			exception.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}

	}
}
