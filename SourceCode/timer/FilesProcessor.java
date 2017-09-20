package timer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import timer.DBOperations;

import org.I0Itec.zkclient.ZkClient;

import com.datastax.driver.core.Session;

public class FilesProcessor {
	// private final static byte PART_SIZE = 120;
	public  Map<String,String> transferMetaData1;
	private final static int PART_SIZE = 64 * 1024 * 1024;
	static ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000,
			ZKStringSerializer$.MODULE$);
	static ConsumerConnector consumerConnector = null;
	public  Session session1;
	public  int publishCount=0;
	public  int subscribeCount=0;
	

	public  void publish(String TOPIC, String Key, String Message, Map<String, String> metadata) {
		// System.out.println("Admin utils :::::;"
		// + AdminUtils.topicExists(zkClient, TOPIC));
		boolean b = false;
		if (AdminUtils.topicExists(zkClient, TOPIC) == b) {
			createTopic(TOPIC);
		}
		// createTopic(TOPIC);
		String Status = "published succesfully";
		Properties ppts = new Properties();
		ppts.put("metadata.broker.list", "localhost:9093");
		ppts.put("serializer.class", "kafka.serializer.StringEncoder");
		ppts.put("reconnect.backoff.ms", "50");
		ppts.put("retry.backoff.ms", "100");
		ppts.put("producer.type", "async");
		ppts.put("message.send.max.retries", "2");
		//ppts.put("message.send.max.retries", "3");
		// ppts.put("log.retention.minutes", 2);
		// ppts.put("auto.offset.reset", "smallest");
		ppts.put("message.max.bytes", "1073741824");
		// replica.fetch.max.bytes=15728640
		// ppts.put("replica.fetch.max.bytes", "1073741824");
		ProducerConfig producerConfig = new ProducerConfig(ppts);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);
		KeyedMessage<String, String> message = new KeyedMessage<String, String>(
				TOPIC, Key, Message);
		producer.send(message);
		publishCount++;
		String incrementPublish=Integer.toString(publishCount);
//		data base code to update  publishCount in transfer_event
		transferMetaData1.put("incrementPublish", incrementPublish);
		Session session=DBOperations.connectCassandra();
		session1=session;
		DBOperations.updateTransferEventPublishDetails(session,transferMetaData1);
		// System.out.println(message);
		// System.out.println(message);
		producer.close();
		consume(TOPIC,metadata);
		DBOperations.updateTransferDetails(session,transferMetaData1,metadata);
		System.out.println("Consumed Successfully");
	}
	
	public  void consume(String TOPIC, Map<String, String> metadata)
	{
		String topic = TOPIC;
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "testgroup");
		// props.put("bootstrap.servers", "localhost:9093");
		// props.put("group.id", "testgroup");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		// props.put("auto.offset.reset", "smallest");
		props.put("fetch.message.max.bytes", "1073741824");
		//props.put("message.send.max.retries", "2");
		// props.put("fetch.message.max.bytes", "52428800");
		ConsumerConfig conConfig = new ConsumerConfig(props);
		consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		topicCount.put(topic, new Integer(1));
		// ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector.createMessageStreams(topicCount);
		// Get Kafka stream for topic 'mytopic'`
		List<KafkaStream<byte[], byte[]>> kStreamList = consumerStreams
				.get(topic);
		// System.out.println(kStreamList);
		// Iterate stream using ConsumerIterator
		for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = kStreams
					.iterator();

			File fl = new File(metadata.get("destinationDirectory")+"\\"+TOPIC);
			transferMetaData1.put("destinationFile", metadata.get("destinationDirectory")+"\\"+TOPIC);
			// String str;
			FileWriter fw;
			while (consumerIte.hasNext()) {
				try {
					fw = new FileWriter(fl, true);
					fw.write(new String(consumerIte.next().message()));
					subscribeCount++;
					String incrementConsumer=Integer.toString(subscribeCount);
					fw.close();
					transferMetaData1.put("incrementConsumer", incrementConsumer);
					DBOperations.updateTransferEventConsumeDetails(session1, transferMetaData1);
					consumerConnector.shutdown();
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		}
	
	// Shutdown the consumer connector
	if (consumerConnector != null)
		consumerConnector.shutdown();
	}

	public  String createTopic(String topicName) {
		// ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000,
		// ZKStringSerializer$.MODULE$);
		// zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
		Properties topicConfig = new Properties();
		AdminUtils.createTopic(zkClient, topicName, 1, 1, topicConfig);
		// zkClient.close();

		return topicName;
	}

	public  String deleteTopic(String topicName) {
		String status = topicName + " : topic is deleted successfully";
		ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000,
				ZKStringSerializer$.MODULE$);
		zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
		return status;
	}

	public  String getMessages(String sPath1, Map<String, String> metadata, Map<String, String> transferMetaData) {
		transferMetaData1 = transferMetaData;
		String status = null;
		String delimiter = "\\\\";
		File inputFile = new File(sPath1);
		FileInputStream inputStream;
		String spath2=sPath1.toString();
		String Key;
		String sfile = null;
		String ss[] = spath2.split(delimiter);
		int c = ss.length;
		sfile = ss[c - 1];
		int fileSize = (int) inputFile.length();
		System.out.println("filesize is"+fileSize);
		int nChunks = 0, read = 0, readLength = PART_SIZE;
		byte[] byteChunkPart;
		try {
			inputStream = new FileInputStream(inputFile);
			while (fileSize > 0) {
				if (fileSize <= 5) {
					readLength = fileSize;
				}
				if (inputStream.available() < readLength) {
					byteChunkPart = new byte[inputStream.available()];
					// IOUtils.copyLarge();
					read = inputStream.read(byteChunkPart, 0,
							inputStream.available());
				} else {
					byteChunkPart = new byte[readLength];
					read = inputStream.read(byteChunkPart, 0, readLength);
				}
				fileSize -= read;
				assert (read == byteChunkPart.length);
				nChunks++;
				Key = sfile + "." + (nChunks - 1);
				System.out.println(sfile);
				 publish(sfile, Key, new String(byteChunkPart),metadata);

			}
			inputStream.close();
			// To Do code to update and insert into DB
		} catch (IOException exception) {
			exception.printStackTrace();
		}
		return status;
	}
}
