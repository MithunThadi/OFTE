package commm;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;

public class kafkaconnect {

	static Logger logger = Logger.getLogger(kafkaconnect.class.getName());
	static String loggerException="Caught exception; decorating with appropriate status template : ";
	public static void createTopic(String topicName, int numberOfPartitions, int numberOfReplications) {
		ZkClient zkClient = null;
		ZkUtils zkUtils = null;
		// String topicName, int numPartitions, int numReplication
		try {
			String zookeeperHosts = "172.25.26.157:2181"; // If multiple zookeeper then -> String zookeeperHosts =
															// "192.168.20.1:2181,192.168.20.2:2181";
			int sessionTimeOutInMs = 15 * 1000; // 15 secs
			int connectionTimeOutInMs = 10 * 1000; // 10 secs

			zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs);
			// Ref: https://gist.github.com/jjkoshy/3842975
			zkClient.setZkSerializer(new ZkSerializer() {
				@Override
				public byte[] serialize(Object o) throws ZkMarshallingError {
					return ZKStringSerializer.serialize(o);
				}

				@Override
				public Object deserialize(byte[] bytes) throws ZkMarshallingError {
					return ZKStringSerializer.deserialize(bytes);
				}
			});

			zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

//			int noOfPartitions = numberOfPartitions;
//			int noOfReplication = numberOfReplications;
			Properties topicConfiguration = new Properties();

			AdminUtils.createTopic(zkUtils, topicName, numberOfPartitions, numberOfReplications, topicConfiguration,
					RackAwareMode.Enforced$.MODULE$);
			System.out.println(AdminUtils.topicExists(zkUtils, topicName));
		}catch (NoAvailableBrokersException noAvailableBrokersException) {
			StringWriter stack = new StringWriter();
			noAvailableBrokersException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (ZkTimeoutException zkTimeoutException) {
			StringWriter stack = new StringWriter();
			zkTimeoutException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}  catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
	}
}
