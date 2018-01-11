package com.ofte.kafka.services;

import java.util.Random;

public class KafkaUtils {
	public static int portGenerator() {
		Random rand = new Random();
		int randomNum = rand.nextInt((30000 - 2000) + 1) + 2000;
		return randomNum;
	}

	public static int idGenerator() {
		Random rand = new Random();
		int randomNum = rand.nextInt((65535 - 1) + 1) + 1;
		return randomNum;
	}
}
