package com.ofte.services;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class ZookeeperServerService {

    NIOServerCnxnFactory factory; 
    File snapshotDir; 
    File logDir; 
    public ZooKeeperServer startup(String zkHost, int zkPort) throws IOException, InterruptedException { 
        
    	snapshotDir = new File("F:\\zootmp1-"+zkPort);
    	logDir = new File("F:\\zootmp1-"+zkPort);
        ZooKeeperServer zkSrv = new ZooKeeperServer(snapshotDir, logDir, 500); 	            
        factory = new NIOServerCnxnFactory(); 	 
        factory.configure(new InetSocketAddress(zkHost, zkPort), 16); 	 
        factory.startup(zkSrv);
        return zkSrv;
    } 
    public int getActualPort() { 
        return factory.getLocalPort(); 
    } 
    public void shutdown() throws IOException { 
        if (factory != null) { 
            factory.shutdown(); 
        } 
    } 

}
