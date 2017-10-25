package kafka.services;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class ZookeeperServerService {

    String zkHost = "locahost"; 
    int zkPort =RandomNumber.randomNumberForZookeeper(); 
    NIOServerCnxnFactory factory; 
    File snapshotDir; 
    File logDir; 
    ZookeeperServerService(String zkHost, int zkPort) { 
        this.zkHost = zkHost; 
        this.zkPort = zkPort; 
    } 
    ZooKeeperServer startup() throws IOException, InterruptedException { 
        
    	snapshotDir = new File("F:\\zootmp1");
    	logDir = new File("F:\\zootmp");
        ZooKeeperServer zkSrv = new ZooKeeperServer(snapshotDir, logDir, 500); 	            
        factory = new NIOServerCnxnFactory(); 	 
        factory.configure(new InetSocketAddress(zkHost, zkPort), 16); 	 
        factory.startup(zkSrv);
        return zkSrv;
    } 
    int getActualPort() { 
        return factory.getLocalPort(); 
    } 
    public String getZKAddress() { 
        return zkHost + ':' + zkPort; 
    } 
    void shutdown() throws IOException { 
        if (factory != null) { 
            factory.shutdown(); 
        } 
    } 

}
