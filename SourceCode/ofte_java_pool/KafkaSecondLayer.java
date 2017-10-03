package ofte;

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
	static ZkClient zkClient = new ZkClient("172.17.3.121:2182", 10000, 10000, ZKStringSerializer$.MODULE$);
	static ConsumerConnector consumerConnector = null;

	public static void publish(String TOPIC, String Key, String Message) {
		long timestamp = System.currentTimeMillis();
		ZkUtils zkutils = new ZkUtils(zkClient, new ZkConnection("172.17.3.121:2182"), true);

		boolean b = false;
		Properties topicConfig = new Properties();
		RackAwareMode r;
		if (AdminUtils.topicExists(zkutils, TOPIC)) {
			AdminUtils.createTopic(zkutils, TOPIC, 1, 3, topicConfig, RackAwareMode.Enforced$.MODULE$);
		}
		// createTopic(TOPIC);

		Properties ppts = new Properties();
		ppts.put("metadata.broker.list", "172.17.3.121:2182:9094");
		ppts.put("serializer.class", "kafka.serializer.StringEncoder");
		ppts.put("reconnect.backoff.ms", "50");
		ppts.put("retry.backoff.ms", "100");
		ppts.put("producer.type", "async");
		ppts.put("message.send.max.retries", "2");
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
		String Status = "published succesfully";
		System.out.println(Status);

		// data base code to update publishCount in transfer_event

		// System.out.println(message);
		producer.close();
		consume(TOPIC);
	}

	public static void consume(String TOPIC) {

		String topic = TOPIC;
		// int consumerCount=0;
		Properties props = new Properties();
		props.put("zookeeper.connect", "172.17.3.121:2182:2182");
		// String id=UUID.randomUUID().toString().replace("-","");
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
		// props.put("fetch.message.max.bytes", "52428800");
		ConsumerConfig conConfig = new ConsumerConfig(props);
		consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(conConfig);
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		topicCount.put(topic, new Integer(1));
		// ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
				.createMessageStreams(topicCount);
		// Get Kafka stream for topic 'mytopic'`
		List<KafkaStream<byte[], byte[]>> kStreamList = consumerStreams.get(topic);
		// System.out.println(kStreamList);
		// Iterate stream using ConsumerIterator
		for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();

			File fl = new File("D:\\Test\\second.txt");
			// transferMetaData1.put("destinationFile",
			// metadata.get("destinationDirectory")+"\\"+TOPIC);
			// Session session=DBOperations.connectCassandra();

			// String str;
			FileWriter fw;
			while (consumerIte.hasNext()) {
				try {
					fw = new FileWriter(fl, true);
					fw.write(new String(consumerIte.next().message()));

					// consumerCount++;
					// String incrementConsumer=Integer.toString(consumerCount);
					// data base code to update consumerCount in transfer_event

					fw.close();
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

	public static String createTopic(String topicName) {
		// ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000,
		// ZKStringSerializer$.MODULE$);
		// zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
		Properties topicConfig = new Properties();
		// 1st one is no of partitions
		// 2nd one is no of replications
		// AdminUtils.createTopic(zkClient, topicName, 1, 1, topicConfig);
		// zkClient.close();

		return topicName;
	}
}
