package com.ofte.file.services;

public class Operations {

	int sum = 0;
	int diff = 0;
	long product = 1;

	public int performAdditionOperation(String[] parameters) {

		System.out.println("Operations in--- ADD");
		for (int i = 0; i < parameters.length; i++) {
			sum = sum + Integer.parseInt(parameters[i]);
		}

		// String c = parameters[0]+parameters[1];
		return sum;
	}

	public int performSubstractionOperation(String[] parameters) {
		// int c = a-b;
		System.out.println("Operations in--- SUB");
		for (int i = 0; i < parameters.length; i++) {
			diff = diff - Integer.parseInt(parameters[i]);
		}
		return diff;
	}

	public long performMultiplicationOperation(String[] parameters) {
		// int c = a-b;
		System.out.println("Operations in--- MUL");
		for (int i = 0; i < parameters.length; i++) {
			product = product * Integer.parseInt(parameters[i]);
		}
		return product;
	}

}
