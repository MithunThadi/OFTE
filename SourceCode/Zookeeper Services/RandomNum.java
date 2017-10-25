package zookeeper.services;

import java.util.Random;

public class RandomNum {
	
	public static int random() {
		//generating a 4 digit integer
		Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((9999 - 1000) + 1) + 1000;
		//int randomPIN = (int)(Math.random()*9999)+100;
//		System.out.println(randomNum);
		return randomNum;
	}
	}
