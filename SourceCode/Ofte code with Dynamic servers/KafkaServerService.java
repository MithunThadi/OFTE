package com.ofte.services;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.codehaus.plexus.util.FileUtils;

//import com.ofte.services.LoadProperties;
//import com.ofte.zookeeper.services.ZookeeperServerService;
//import com.ofte.zookeeper.services.ZookeeperUtils;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

@SuppressWarnings("deprecation")
public class KafkaServerService {

     String ZK_HOST = "localhost"; 
      static int BROKER_PORT ; 
     int ZK_CONNECTION_TIMEOUT = 6000; 
     int ZK_SESSION_TIMEOUT = 6000; 
      static int id ;
     public static int getBROKER_PORT() {
		return BROKER_PORT;
	}
	public static void setBROKER_PORT(int bROKER_PORT) {
		int BROKER_PORT1=KafkaUtils.portGenerator();
		BROKER_PORT = BROKER_PORT1;
	}
	public static int getZkPort() {
		return zkPort;
	}
	public static void setZkPort(int zkPort) {
		int zkPort1=ZookeeperUtils.portGenerator();
		KafkaServerService.zkPort = zkPort1;
	}
	public static void setId(int id) {
		int id1=	KafkaUtils.idGenerator();
		KafkaServerService.id = id1;
	}
	String zkHost = "localhost"; 
      static int zkPort;
	 String logDir; 
    KafkaConfig kafkaConfig; 
    KafkaServerStartable kafkaSrv; 
    ZkClient zkClient;  
    ZkUtils zkutils;
    LoadProperties loadProperties = new LoadProperties();
    ZookeeperServerService zookeeperServerService = new ZookeeperServerService();
//    public KafkaServerService() {
//    	
//    }
 
    public ZkClient setupEmbeddedZooKeeper() throws IOException, InterruptedException { 
    	
//    	zkPort=zkPort1;
    	zookeeperServerService.startup(zkHost, zkPort); 
//        zkPort = zookeeperServerService.getActualPort(); 
        System.out.println("ZookeeperPort:"+zkPort +"      in kafkasever sevice");
        zkClient = new ZkClient(zkHost + ':' + zkPort, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$); 
        System.out.println("Zookeeper Connected"); 
        return zkClient;
    } 
    public ZkUtils accessZkUtils() {
    	 zkutils = new ZkUtils(zkClient, new ZkConnection(zkHost + ':' + zkPort), false);
    	 System.out.println("zkutils in");
		return zkutils; 	
    }
    
    
    public KafkaConfig setupEmbeddedKafkaServer() throws IOException { 
    	kafkaConfig = new KafkaConfig(getBrokerConfig()); 
    	
//    	BROKER_PORT=BROKER_PORT1;
        System.out.println("KafkaPort:"+BROKER_PORT+"    in kafka server service");
    	kafkaSrv = new KafkaServerStartable(kafkaConfig);
        kafkaSrv.startup(); 
        System.out.println("Kafka Connected"); 
        return kafkaConfig;
    }
    
    public  Properties getBrokerConfig() throws IOException { 
//    int id1=	KafkaUtils.idGenerator();
//    id=id1;
    	logDir = "F:\\kafka-"+BROKER_PORT+"-logs-zk-"+zkPort;
        Properties properties = new Properties(); 
        properties.put("reserved.broker.max.id", id);
        properties.put("broker.id", id); 
        properties.put("host.name", ZK_HOST); 
        properties.put("port",  BROKER_PORT); 
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect",zkHost + ':' + zkPort); 
        properties.put("log.flush.interval.messages", "1"); 
        properties.put("replica.socket.timeout.ms", "1500"); 
 
        return properties; 
    } 
	public ProducerConfig getProducerConfig() { 
		LoadProperties loadProperties = new LoadProperties();
		Properties properties = new Properties();
		properties.put("metadata.broker.list",ZK_HOST+":"+BROKER_PORT );
		properties.put("serializer.class", new String(loadProperties.getKafkaProperties().getProperty("SERIALIZER.CLASS")));
		properties.put("key.serializer.class", new String(loadProperties.getKafkaProperties().getProperty("SERIALIZER.CLASS")));
		properties.put("reconnect.backoff.ms",(String) loadProperties.getKafkaProperties().getProperty("RECONNECT.BACKOFF.MS"));
		properties.put("retry.backoff.ms",(String) loadProperties.getKafkaProperties().getProperty("RETRY.BACKOFF.MS"));
		properties.put("producer.type",(String) loadProperties.getKafkaProperties().getProperty("PRODUCER.TYPE"));
		properties.put("message.send.max.retries",(String) loadProperties.getKafkaProperties().getProperty("MESSAGE.SEND.MAX.RETRIES"));
		properties.put("message.max.bytes",(String) loadProperties.getKafkaProperties().getProperty("MESSAGE.MAX.BYTES"));
	 
	        return new ProducerConfig(properties); 
	    } 
	public ConsumerConfig getConsumerConfig() {
		LoadProperties loadProperties = new LoadProperties();
    	//int id = KafkaUtils.idGenerator();
		Properties properties = new Properties();
		properties.put("zookeeper.connect",zkHost + ':' + zkPort);
		properties.put("group.id", id);
		properties.put("enable.auto.commit",loadProperties.getKafkaProperties().getProperty("ENABLE.AUTO.COMMIT"));
		properties.put("auto.commit.interval.ms", loadProperties.getKafkaProperties().getProperty("AUTO.COMMIT.INTERVAL.MS"));
		properties.put("auto.offset.reset", loadProperties.getKafkaProperties().getProperty("AUTO.OFFSET.RESET"));
		properties.put("session.timeout.ms", loadProperties.getKafkaProperties().getProperty("SESSION.TIMEOUT.MS"));
		properties.put("key.deserializer", loadProperties.getKafkaProperties().getProperty("KEY.DESERIALIZER"));
		properties.put("value.deserializer", loadProperties.getKafkaProperties().getProperty("VALUE.DESERIALIZER"));
		properties.put("fetch.message.max.bytes", loadProperties.getKafkaProperties().getProperty("FETCH.MESSAGE.MAX.BYTES"));
		//Creation of ConsumerConfig object 
		ConsumerConfig consumerConfig = new ConsumerConfig(properties);
		return consumerConfig;
		
	}
	 public  String getZKAddress() { 
	        return zkHost + ':' + zkPort; 
	    }
	 public String getId() { 
	        return String.valueOf(id); 
	    }
	  public String getBrokerAddress() { 
	        return ZK_HOST + ':' + BROKER_PORT; 
	    }
	  public void shutdown() { 
	        try {
	        if (kafkaSrv != null) { 
	        	System.out.println("Try to shutdown");
	            kafkaSrv.shutdown(); 
	            System.out.println("Kafka Disconnected"); 
	        }
//	        if (zkClient != null) { 
//	            zkClient.close(); 
//	            System.out.println("zkClient closed");
//	        }
	        if (zookeeperServerService != null) { 	 
	            try { 
	            	zookeeperServerService.shutdown(); 
	            	System.out.println("zookeeperServerService shutdown");
	            } 
	            catch (Exception e) { 
	               e.printStackTrace();
	            } 	 
//	            zookeeperServerService = null; 
	        } }catch (Exception e) { 
	               e.printStackTrace();
	            }finally {
	        	System.out.println("logDir deleting");
//	        	try {
	        		 if (kafkaSrv != null) { 
	     	        	System.out.println("Try to shutdown");
	     	            kafkaSrv.shutdown(); 
	     	            System.out.println("Kafka Disconnected"); 
//	     	        }
//					FileUtils.forceDelete(new File(logDir));
				} 
//	        		 catch (IOException e) {
//					e.printStackTrace();
//				}
	        	System.out.println("logDir deleted");
	        }
	    } 
//	  public static void main(String[] args) throws IOException, InterruptedException {
//		KafkaServerService kafkaServerService = new KafkaServerService();
//		kafkaServerService.setupEmbeddedZooKeeper();
//		kafkaServerService.setupEmbeddedKafkaServer();
//		kafkaServerService.shutdown();
//		//new File(logDir);
//		System.out.println("done");
//		
//	}
}
