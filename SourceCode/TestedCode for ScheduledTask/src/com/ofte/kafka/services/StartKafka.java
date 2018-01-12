package com.ofte.kafka.services;

import java.io.IOException;

public class StartKafka {
	public static void main(String[] args) {
		try {
			// Process p = Runtime.getRuntime().exec("cmd /c start
			// \"F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\bin\\windows\\kafka-server-start.bat
			// config\\newzookeeperserver.properties\"") ;
			// Process p1 = Runtime.getRuntime().exec("cmd /c start", null, new
			// File("F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\bin\\windows\\kafka-server-start.bat
			// config\\newzookeeperserver.properties"));
			Runtime.getRuntime().exec(
					"cmd /c start \"\" F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\bin\\windows\\zookeeper-server-start.bat F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\config\\newzookeeperserver.properties");
			Runtime.getRuntime().exec(
					"cmd /c start \"\" F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\bin\\windows\\kafka-server-start.bat F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\config\\newkafkaserver.properties");
			System.out.println("started");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}