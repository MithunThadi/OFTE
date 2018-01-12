package com.ofte.file.utility;

import java.util.Map;

public class Substraction {

	// static int diff = 0;
	public static int performOFTETask(String[] parameters,
			Map<String, String> metaDataMap,
			Map<String, String> transferMetaData) {
		int diff = 0;
		System.out.println("Operations in--- SUB");
		// String[] split = parameters.split(",");
		for (int i = 0; i < parameters.length; i++) {
			diff = diff - Integer.parseInt(parameters[i]);
		}
		// String c = parameters[0]+parameters[1];
		System.out.println(diff);
		return diff;
	}
}
