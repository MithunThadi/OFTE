package zookeeper.services;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import kafka.utils.ZKStringSerializer$;

public class ZookeeperServerService {
	 private static final String ZK_HOST = "localhost";  
	    private static final int ZK_CONNECTION_TIMEOUT = 6000;  
	    private static final int ZK_SESSION_TIMEOUT = 6000; 
	    private static boolean zkReady; 
	    private static ZkClient zkClient;
		private static String zkHost = "localhost";
		private static int zkPort = RandomNum.random();
		 NIOServerCnxnFactory factory;
//	    private EmbeddedZooKeeper zooKeeper;
	    ZookeeperServerService(String zkHost, int zkPort) { 
            this.zkHost = zkHost; 
            this.zkPort = zkPort; 
        }
//	    private void setupEmbeddedZooKeeper() throws IOException, InterruptedException 
//        { 
//            
////            zooKeeper.startup(); 
////            zkPort = zooKeeper.getActualPort(); 
//            System.out.println(zkPort);
//            zkClient = new ZkClient(getZKAddress(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$); 
//            zkReady = true; 
//        }
	    public static String getZKAddress() 
        { 
            return ZK_HOST + ':' + zkPort; 
        } 
        
         public NIOServerCnxnFactory startup() throws IOException, InterruptedException 
         { 
            File snapshotDir = new File("D:\\softwares\\Kafka_latest\\zoo");
            File logdir = new File("D:\\softwares\\Kafka_latest\\zoo1");
            ZooKeeperServer zkSrv = new ZooKeeperServer(snapshotDir,logdir, 500); 
            factory = new NIOServerCnxnFactory(); 
            factory.configure(new InetSocketAddress(zkHost, zkPort), 16); 
            factory.startup(zkSrv); 
            return factory;
         } 
 
         public int getActualPort() { 
            return factory.getLocalPort(); 
        } 
 
//        
	    public static void main(String[] args) throws IOException, InterruptedException {
			ZookeeperServerService zookeeperServerService = new ZookeeperServerService(zkHost, zkPort);
//			zookeeperServerService.setupEmbeddedZooKeeper();
			NIOServerCnxnFactory factory = zookeeperServerService.startup();
			zookeeperServerService.getActualPort();
			 System.out.println(zkPort);
			zkClient = new ZkClient(getZKAddress(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$); 
			System.out.println("Zookeeper Connected");
            zkReady = true; 
            factory.shutdown();
            System.out.println("Zookeeper closed");
		}
	    
}
