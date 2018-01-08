package com.ofte.kafka.services;

import java.util.Random;

public class ZookeeperUtils {
	public static int portGenerator() {
		Random rand = new Random();
		int randomNum = rand.nextInt((9999 - 1) + 1) + 1;
		return randomNum;
	}
}
