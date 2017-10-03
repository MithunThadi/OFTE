package ofte;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import com.datastax.driver.core.Session;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class FilesProcessor {
	private final static int PART_SIZE = 32 * 1024 * 1024;
	static ZkClient zkClient = new ZkClient("172.17.3.121:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
	static ConsumerConnector consumerConnector = null;
	// private final static byte PART_SIZE = 120;
	public int publishCount = 0;
	public int subscribeCount = 0;

	public void publish(String TOPIC, String Key, String Message, Map<String, String> metadata,
			Map<String, String> transferMetaData) {

		long timestamp = System.currentTimeMillis();
		System.out.println("setting zkclient");
		ZkUtils zkutils = new ZkUtils(zkClient, new ZkConnection("172.17.3.121:2181"), true);
		if (!AdminUtils.topicExists(zkutils, TOPIC)) {
			kafkaconnect.createTopic(TOPIC, 1, 1);
		}
		System.out.println("created success");
		String Status = "published succesfully";
		Properties ppts = new Properties();
		ppts.put("metadata.broker.list", "localhost:9092");
		ppts.put("serializer.class", "kafka.serializer.StringEncoder");
		ppts.put("reconnect.backoff.ms", "50");
		ppts.put("retry.backoff.ms", "100");
		ppts.put("producer.type", "async");
		ppts.put("message.send.max.retries", "2");
		// ppts.put("message.send.max.retries", "3");
		// ppts.put("log.retention.minutes", 2);
		// ppts.put("auto.offset.reset", "smallest");
		ppts.put("message.max.bytes", "1073741824");
		// replica.fetch.max.bytes=15728640
		// ppts.put("replica.fetch.max.bytes", "1073741824");
		ProducerConfig producerConfig = new ProducerConfig(ppts);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);
		KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, Key, Message);
		producer.send(message);
		publishCount++;
		String incrementPublish = Integer.toString(publishCount);
		// data base code to update publishCount in transfer_event
		transferMetaData.put("incrementPublish", incrementPublish);
		Session session = DBOperations.connectCassandra();
		DBOperations.updateTransferEventPublishDetails(session, transferMetaData);
		producer.close();
		Lock lock = new ReentrantLock();
		lock.lock();
		consume(TOPIC, metadata, session, transferMetaData);
		System.out.println("Consumed Successfully: " + TOPIC);
		DBOperations.updateTransferDetails(session, transferMetaData, metadata);
		System.out.println("updated cass: " + TOPIC);
		session.close();
		session.closeAsync();
		lock.unlock();
		System.out.println("unlocking");

	}

	public void consume(String TOPIC, Map<String, String> metadata, Session session,
			Map<String, String> transferMetaData) {

		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		Properties props = new Properties();
		props.put("zookeeper.connect", "172.17.3.121:2181");
		props.put("group.id", "testgroup");
		// props.put("bootstrap.servers", "localhost:9093");
		// props.put("group.id", "testgroup");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// props.put("auto.offset.reset", "smallest");
		props.put("fetch.message.max.bytes", "1073741824");
		// props.put("message.send.max.retries", "2");
		// props.put("fetch.message.max.bytes", "52428800");
		ConsumerConfig conConfig = new ConsumerConfig(props);

		// consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
		consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(conConfig);

		topicCount.put(TOPIC, new Integer(1));
		// ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
				.createMessageStreams(topicCount);
		// Get Kafka stream for topic 'mytopic'`
		List<KafkaStream<byte[], byte[]>> kStreamList = consumerStreams.get(TOPIC);
		for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
			transferMetaData.put("destinationFile", metadata.get("destinationDirectory") + "\\" + TOPIC);
			FileWriter destinationFileWriter;
			while (consumerIte.hasNext()) {
				try {
					destinationFileWriter = new FileWriter(
							new File(metadata.get("destinationDirectory") + "\\" + TOPIC), true);
					destinationFileWriter.write(new String(consumerIte.next().message()));
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
		String status = topicName + " : topic is deleted successfully";
		ZkClient zkClient = new ZkClient("172.17.3.121:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
		zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
		return status;
	}

	public void getMessages(String sPath1, Map<String, String> metadata, Map<String, String> transferMetaData) {

		String delimiter = "\\\\";
		File inputFile = new File(sPath1);
		FileInputStream inputStream;
		String spath2 = sPath1.toString();
		String Key;
		String sfile = null;
		String ss[] = sPath1.split(delimiter);
		int c = ss.length;
		sfile = ss[c - 1];
		int fileSize = (int) inputFile.length();
		System.out.println("filesize is" + fileSize);
		int nChunks = 0, read = 0, readLength = PART_SIZE;
		byte[] byteChunkPart;
		try {
			inputStream = new FileInputStream(inputFile);
			while (fileSize > 0) {

				if (inputStream.available() < readLength) {
					byteChunkPart = new byte[inputStream.available()];
					// IOUtils.copyLarge();
					read = inputStream.read(byteChunkPart, 0, inputStream.available());
				} else {
					byteChunkPart = new byte[readLength];
					read = inputStream.read(byteChunkPart, 0, readLength);
				}
				fileSize -= read;
				assert (read == byteChunkPart.length);
				nChunks++;
				Key = sfile + "." + (nChunks - 1);
				System.out.println(sfile);
				Lock lock = new ReentrantLock();
				lock.lock();
				publish(sfile, Key, new String(byteChunkPart), metadata, transferMetaData);
				lock.unlock();
				System.out.println("completed for thread: " + sfile);

			}
			inputStream.close();
			System.out.println("closing Stream for " + inputFile);
			// To Do code to update and insert into DB
		} catch (IOException exception) {
			exception.printStackTrace();
		}

	}
}
