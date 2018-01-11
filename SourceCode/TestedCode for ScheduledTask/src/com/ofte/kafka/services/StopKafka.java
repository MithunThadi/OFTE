package com.ofte.kafka.services;

import java.io.IOException;

public class StopKafka {
	public static void main(String[] args) {
		try {
			Runtime.getRuntime().exec(
					"cmd /c start \"\" F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\bin\\windows\\kafka-server-stop.bat");
			Runtime.getRuntime().exec(
					"cmd /c start \"\" F:\\kafka-0.11.0.1-src\\kafka-0.11.0.1-src\\bin\\windows\\zookeeper-server-stop.bat");
			System.out.println("stopped");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
