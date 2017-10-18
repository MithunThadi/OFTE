package commm;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

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

public class KafkaSecondLayer {
	 ZkClient zkClient = new ZkClient("localhost:2182", 10000, 10000, ZKStringSerializer$.MODULE$);
	 ConsumerConnector consumerConnector = null;

	public void publish(String TOPIC, String Key, String Message) {
//		long timestamp = System.currentTimeMillis();
		ZkUtils zkutils = new ZkUtils(zkClient, new ZkConnection("localhost:2182"), true);

//		boolean b = false;
		//Properties topicConfig = new Properties();
//		RackAwareMode r;
		if (!AdminUtils.topicExists(zkutils, TOPIC)) {
			//AdminUtils.createTopic(zkutils, TOPIC, 1, 3, topicConfig, RackAwareMode.Enforced$.MODULE$);
			KafkaSecondeLayerConnect kafkaSecondeLayerConnect=new KafkaSecondeLayerConnect();
			kafkaSecondeLayerConnect.createTopic(TOPIC, 1, 1);
		}
		// createTopic(TOPIC);

		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9093");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("reconnect.backoff.ms", "50");
		properties.put("retry.backoff.ms", "100");
		properties.put("producer.type", "async");
		properties.put("message.send.max.retries", "2");
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
		String Status = "published succesfully";
		System.out.println(Status);

		// data base code to update publishCount in transfer_event

		// System.out.println(message);
		producer.close();
		KafkaSecondLayer kafkaSecondLayer=new KafkaSecondLayer();
		kafkaSecondLayer.consume(TOPIC);
	}

	public void consume(String TOPIC) {

		String topicName = TOPIC;
		// int consumerCount=0;
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "localhost:2182");
		// String id=UUID.randomUUID().toString().replace("-","");
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
		// props.put("fetch.message.max.bytes", "52428800");
		ConsumerConfig conConfig = new ConsumerConfig(properties);
		consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(conConfig);
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		topicCount.put(topicName, new Integer(1));
		// ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
				.createMessageStreams(topicCount);
		// Get Kafka stream for topic 'mytopic'`
		List<KafkaStream<byte[], byte[]>> kafkaStreamList = consumerStreams.get(topicName);
		// System.out.println(kStreamList);
		// Iterate stream using ConsumerIterator
		for (final KafkaStream<byte[], byte[]> kafkaStreams : kafkaStreamList) {
			ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStreams.iterator();

			File file = new File("D:\\open\\Secondlayer\\second.txt");
			// transferMetaData1.put("destinationFile",
			// metadata.get("destinationDirectory")+"\\"+TOPIC);
			// Session session=DBOperations.connectCassandra();

			// String str;
			FileWriter fileWriter;
			while (consumerIterator.hasNext()) {
				try {
					fileWriter = new FileWriter(file, true);
					fileWriter.write(new String(consumerIterator.next().message()));

					// consumerCount++;
					// String incrementConsumer=Integer.toString(consumerCount);
					// data base code to update consumerCount in transfer_event

					fileWriter.close();
					// transferMetaData1.put("incrementConsumer", incrementConsumer);
					// DBOperations.updateTransferEventConsumeDetails(session1, transferMetaData1);
					consumerConnector.shutdown();
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		}

		// Shut down the Consumer Connector
		if (consumerConnector != null)
			consumerConnector.shutdown();
	}

//	public static String createTopic(String topicName) {
////		ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
////		zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
//		Properties topicConfig = new Properties();
////		AdminUtils.createTopic(zkutils, topicName, 1, 3, topicConfig, RackAwareMode.Enforced$.MODULE$);
////		zkClient.close();
//
//		return topicName;
//
////		return topicName;
//	}
}
