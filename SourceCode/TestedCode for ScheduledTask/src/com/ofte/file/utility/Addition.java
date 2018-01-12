package com.ofte.file.utility;

import java.util.Map;

public class Addition {

	// static int sum = 0;

	public static int performOFTETask(String[] parameters,
			Map<String, String> metaDataMap,
			Map<String, String> transferMetaData) {
		int sum = 0;
		System.out.println("Operations in--- ADD");
		// String[] split = parameters.split(",");
		for (int i = 0; i < parameters.length; i++) {
			sum = sum + Integer.parseInt(parameters[i]);
		}
		// String c = parameters[0]+parameters[1];
		System.out.println(sum);
		return sum;
	}
	// public int performAdditionOperation(String[] parameters) {
	//
	// System.out.println("Operations in--- ADD");
	// for(int i=0;i<parameters.length;i++) {
	// sum = sum + Integer.parseInt(parameters[i]);
	// }
	//
	//// String c = parameters[0]+parameters[1];
	// return sum;
	// }
	//
	// public int performSubstractionOperation(String[] parameters) {
	//// int c = a-b;
	// System.out.println("Operations in--- SUB");
	// for(int i=0;i<parameters.length;i++) {
	// diff = diff - Integer.parseInt(parameters[i]);
	// }
	// return diff;
	// }

}
