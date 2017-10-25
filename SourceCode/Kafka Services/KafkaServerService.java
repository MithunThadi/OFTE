package kafka.services;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
//import kafka.services.KafkaEmbeddedBroker.EmbeddedZooKeeper;
import kafka.utils.SystemTime$;
import kafka.utils.ZKStringSerializer$;
//import kafka.server.KafkaServer;
//import kafka.services.KafkaEmbeddedBroker.EmbeddedZooKeeper;
import kafka.utils.StaticPartitioner;

public class KafkaServerService {

    private static final String ZK_HOST = "localhost"; 
    private static final int BROKER_PORT = RandomNumber.randomNumberForKafka(); 
    private static final int ZK_CONNECTION_TIMEOUT = 6000; 
    private static final int ZK_SESSION_TIMEOUT = 6000; 
    private static final String zkHost = "localhost"; 
    private static int zkPort = RandomNumber.randomNumberForZookeeper(); 
    private boolean zkReady; 
    private KafkaConfig brokerCfg; 
    private KafkaServerStartable kafkaSrv; 
    private ZkClient zkClient;  
 
    private void setupEmbeddedZooKeeper() throws IOException, InterruptedException { 

		//        EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper(ZK_HOST, zkPort); 
    	ZookeeperServerService zookeeperServerService = new ZookeeperServerService(zkHost, zkPort);
    	zookeeperServerService.startup(); 
        zkPort = zookeeperServerService.getActualPort(); 
        System.out.println("ZookeeperPort:"+zkPort);
 
        zkClient = new ZkClient(zookeeperServerService.getZKAddress(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$); 
        System.out.println("Zookeeper Connected");
        zkReady = true; 
    } 
    
    private void setupEmbeddedKafkaServer() throws IOException { 
        brokerCfg = new KafkaConfig(getBrokerConfig()); 
        System.out.println("KafkaPort:"+BROKER_PORT);
    	kafkaSrv = new KafkaServerStartable(brokerCfg);
        kafkaSrv.startup(); 
        System.out.println("Kafka Connected");        
    }
    
    private static Properties getBrokerConfig() throws IOException { 
        Properties props = new Properties(); 
        props.put("broker.id", "0"); 
        props.put("host.name", ZK_HOST); 
        props.put("port", "" + BROKER_PORT); 
        props.put("log.dir", "F:\\tmp2");
        props.put("zookeeper.connect",getZKAddress()); 
        props.put("log.flush.interval.messages", "1"); 
        props.put("replica.socket.timeout.ms", "1500"); 
 
        return props; 
    } 
	private static ProducerConfig getProducerConfig() { 
	        Properties props = new Properties(); 
	        props.put("metadata.broker.list", getBrokerAddress()); 
	        props.put("serializer.class", StringEncoder.class.getName()); 
	        props.put("key.serializer.class", StringEncoder.class.getName()); 
	        props.put("reconnect.backoff.ms", "50");
	        props.put("retry.backoff.ms", "100");
	        props.put("producer.type", "async");
	        props.put("message.send.max.retries", "2");
	        props.put("message.max.bytes", "1073741824");
	 
	        return new ProducerConfig(props); 
	    } 
	 public static String getZKAddress() { 
	        return ZK_HOST + ':' + zkPort; 
	    }
	  private static String getBrokerAddress() { 
	        return ZK_HOST + ':' + BROKER_PORT; 
	    }
	  public void shutdown() { 
	        zkReady = false; 
	 
	        if (kafkaSrv != null) { 
	        	System.out.println("Try to shutdown");
	            kafkaSrv.shutdown(); 
	            System.out.println("Kafka Disconnected"); 
	        }
	        if (zkClient != null) { 
	            zkClient.close(); 
	        }
	        ZookeeperServerService zookeeperServerService = new ZookeeperServerService(zkHost, zkPort);
	        if (zookeeperServerService != null) { 	 
	            try { 
	            	zookeeperServerService.shutdown(); 
	            } 
	            catch (IOException e) { 
	                // No-op. 
	            } 	 
	            zookeeperServerService = null; 
	        } 
	    } 
	  public static void main(String[] args) throws IOException, InterruptedException {
		KafkaServerService kafkaServerService = new KafkaServerService();
		kafkaServerService.setupEmbeddedZooKeeper();
		kafkaServerService.setupEmbeddedKafkaServer();
		
		
	}
}
