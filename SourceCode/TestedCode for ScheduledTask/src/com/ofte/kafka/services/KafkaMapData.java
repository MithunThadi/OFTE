package com.ofte.kafka.services;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import com.ofte.file.services.LoadProperties;

import kafka.admin.AdminUtils;
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
 * ClassFunctionality: This class is used to publish and consume the metadata
 * mapdata Methods: public void publish(String TOPIC, String Key, String
 * Message) public String consume(String TOPIC)
 */
@SuppressWarnings("deprecation")
public class KafkaMapData {
	// Creating an object for LoadProperties class
	String hostIP = null;
	public KafkaMapData() {
		try {
			InetAddress host = InetAddress.getLocalHost();
			hostIP = host.getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	LoadProperties loadProperties = new LoadProperties();

	// // Creation of ZkClient object and initialising it with loadProperties
	// file
	// ZkClient zkClient = new ZkClient(
	// hostIP + loadProperties.getSecondLayerProperties()
	// .getProperty("ZOOKEEPERHOSTS"),
	// Integer.parseInt(loadProperties.getSecondLayerProperties()
	// .getProperty("SESSIONTIMEOUT")),
	// Integer.parseInt(loadProperties.getSecondLayerProperties()
	// .getProperty("CONNECTIONTIMEOUT")),
	// ZKStringSerializer$.MODULE$);
	// Declaration of parameter ConsumerConnector and initialising it to null
	ConsumerConnector consumerConnector = null;

	/**
	 * This method is used to publish the data
	 * 
	 * @param TOPIC
	 * @param Key
	 * @param Message
	 */
	public void publish(String TOPIC, String Key, String Message) {
		// Creation of ZkUtils object and initialising it with loadProperties
		// file
		// Creation of ZkClient object and initialising it with loadProperties
		// file
		ZkClient zkClient = new ZkClient(
				hostIP + loadProperties.getSecondLayerProperties()
						.getProperty("ZOOKEEPERHOSTS"),
				Integer.parseInt(loadProperties.getSecondLayerProperties()
						.getProperty("SESSIONTIMEOUT")),
				Integer.parseInt(loadProperties.getSecondLayerProperties()
						.getProperty("CONNECTIONTIMEOUT")),
				ZKStringSerializer$.MODULE$);
		ZkUtils zkutils = new ZkUtils(zkClient,
				new ZkConnection(
						hostIP + loadProperties.getSecondLayerProperties()
								.getProperty("ZOOKEEPERHOSTS")),
				true);
		// if loop to check the condition topicExists or not
		if (!AdminUtils.topicExists(zkutils, TOPIC)) {
			// Creating an object for KafkaSecondLayerConnectService class
			KafkaSecondLayerConnectService kafkaSecondeLayerConnect = new KafkaSecondLayerConnectService();
			// Creation of topic
			kafkaSecondeLayerConnect.createTopic(TOPIC,
					Integer.parseInt(loadProperties.getSecondLayerProperties()
							.getProperty("NUMBEROFPARTITIONS")),
					Integer.parseInt(loadProperties.getSecondLayerProperties()
							.getProperty("NUMBEROFREPLICATIONS")));
		}
		// Creation of object properties
		Properties properties = new Properties();
		properties.put("metadata.broker.list",
				hostIP + loadProperties.getSecondLayerProperties()
						.getProperty("METADATA.BROKER.LIST"));
		properties.put("serializer.class", loadProperties
				.getSecondLayerProperties().getProperty("SERIALIZER.CLASS"));
		properties.put("reconnect.backoff.ms",
				loadProperties.getSecondLayerProperties()
						.getProperty("RECONNECT.BACKOFF.MS"));
		properties.put("retry.backoff.ms", loadProperties
				.getSecondLayerProperties().getProperty("RETRY.BACKOFF.MS"));
		properties.put("producer.type", loadProperties
				.getSecondLayerProperties().getProperty("PRODUCER.TYPE"));
		properties.put("message.send.max.retries",
				loadProperties.getSecondLayerProperties()
						.getProperty("MESSAGE.SEND.MAX.RETRIES"));

		properties.put("message.max.bytes", loadProperties
				.getSecondLayerProperties().getProperty("MESSAGE.MAX.BYTES"));
		// Creation of ProducerConfig object
		ProducerConfig producerConfig = new ProducerConfig(properties);
		// Creation of Producer object
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);
		// Creation of KeyedMessage object
		KeyedMessage<String, String> message = new KeyedMessage<String, String>(
				TOPIC, Key, Message);
		// Sending the messages to producer
		producer.send(message);
		// Declaration of parameter Status
		String Status = "published succesfully";
		System.out.println(Status);
		// closing the producer
		producer.close();

	}

	/**
	 * This method is used to consume the data
	 * 
	 * @param TOPIC
	 * @return topicData
	 */
	public String consume(String TOPIC) {
		// Declaration of parameter topicName and initialising it with TOPIC
		String topicName = TOPIC;
		// Declaration of parameter topicData and initialising it with null
		String topicData = null;
		System.out.println("enterd in kafkamapdata consume");
		// Creation of object Properties
		Properties properties = new Properties();
		properties.put("zookeeper.connect", hostIP + loadProperties
				.getSecondLayerProperties().getProperty("ZOOKEEPER.CONNECT"));
		// String id=UUID.randomUUID().toString().replace("-","");
		properties.put("group.id", UUID.randomUUID().toString());
		// props.put("bootstrap.servers", "localhost:9093");
		// props.put("group.id", "testgroup");
		properties.put("enable.auto.commit", loadProperties
				.getSecondLayerProperties().getProperty("ENABLE.AUTO.COMMIT"));
		properties.put("auto.commit.interval.ms",
				loadProperties.getSecondLayerProperties()
						.getProperty("AUTO.COMMIT.INTERVAL.MS"));
		properties.put("auto.offset.reset", loadProperties
				.getSecondLayerProperties().getProperty("AUTO.OFFSET.RESET"));
		properties.put("session.timeout.ms", loadProperties
				.getSecondLayerProperties().getProperty("SESSION.TIMEOUT.MS"));
		properties.put("key.deserializer", loadProperties
				.getSecondLayerProperties().getProperty("KEY.DESERIALIZER"));
		properties.put("value.deserializer", loadProperties
				.getSecondLayerProperties().getProperty("VALUE.DESERIALIZER"));
		// props.put("auto.offset.reset", "smallest");
		properties.put("fetch.message.max.bytes",
				loadProperties.getSecondLayerProperties()
						.getProperty("FETCH.MESSAGE.MAX.BYTES"));
		// props.put("fetch.message.max.bytes", "52428800");
		// Creation of ConsumerConfig object
		ConsumerConfig conConfig = new ConsumerConfig(properties);
		// Creating the consumerConnector
		consumerConnector = kafka.consumer.Consumer
				.createJavaConsumerConnector(conConfig);
		// Creation of Map object
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		// Inserting the values to topicCount
		topicCount.put(topicName, new Integer(1));
		// ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector
				.createMessageStreams(topicCount);
		// Creation of List for kafkaStreamList
		List<KafkaStream<byte[], byte[]>> kafkaStreamList = consumerStreams
				.get(topicName);
		// for each loop to iterate stream using ConsumerIterator
		for (final KafkaStream<byte[], byte[]> kafkaStreams : kafkaStreamList) {
			ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStreams
					.iterator();
			// while to to iterate consumerIterator
			while (consumerIterator.hasNext()) {
				try {
					// Consuming the mapdata
					topicData = new String(consumerIterator.next().message());
					// shutdown consumerConnector
					consumerConnector.shutdown();
				} catch (Exception e) {
					System.out.println(e);
				}
			}

		}
		// shutdown consumerConnector
		consumerConnector.shutdown();
		// return statement
		return topicData;
	}

}
