package com.ofte.file.services;

import java.util.Map;
/**
 * 
 * Class Functionality: This class is used to replace the file extension part
 * Methods: public String variableSubstitutor(Map<String, String>
 * transferMetaDataMap, String sourceFilePattern) public String replacer(String
 * sourceFilePattern, String fileNameORfilePath, int i)
 *
 */
public class VariablesSubstitution {
	/**
	 * This method returns the string based on the delimiter and token value
	 * given in the sourcefilepattern.
	 * 
	 * @param transferMetaDataMap
	 * @param pattern
	 * @return sourceFilePattern
	 */

	public String variableSubstitutor(Map<String, String> transferMetaDataMap,
			String pattern) {

		// Declaration of parameter FileName and initialising it by getting
		// FileName from transferMetaDataMap
		String FileName = transferMetaDataMap.get("FileName");
		// Declaration of parameter FilePath and initialising it by getting
		// FilePath from transferMetaDataMap
		String FilePath = transferMetaDataMap.get("FilePath");
		// for loop to increment i value until it is less than
		// sourceFilePattern.length()
		for (int i = 0; i < pattern.length(); i++) {
			// if loop to check sourceFilePattern.charAt(i) is equal to #
			if (pattern.charAt(i) == '#') {
				// switch case for selecting FileName or FilePath
				switch (pattern.substring(i + 2,
						pattern.indexOf("("))) {
					case "FileName" :
						pattern = replacer(pattern,
								FileName, i);
						System.out.println(pattern);
						break;
					case "FilePath" :
						pattern = replacer(pattern,
								FilePath, i);
						break;
					default :
						break;
				}
			}

		}
		System.out.println(pattern);
		// return statement
		return pattern;

	}
	/**
	 * 
	 * @param sourceFilePattern
	 * @param fileNameORfilePath
	 * @param i
	 * @return
	 */
	public String replacer(String sourceFilePattern, String fileNameORfilePath,
			int i) {
		// Declaration of parameter delimiter
		String delimiter = sourceFilePattern.substring(
				sourceFilePattern.indexOf("(") + 1,
				sourceFilePattern.indexOf("(") + 2);
		// if loop to check delimiter
		if (delimiter.equalsIgnoreCase(".")) {
			delimiter = "\\.";
		} else if (delimiter.equalsIgnoreCase("\\")) {
			delimiter = "\\\\";
		}
		// Declaring and initialising tokenValue for setting variable
		int tokenValue = Integer.parseInt(
				sourceFilePattern.substring(sourceFilePattern.indexOf("(") + 3,
						sourceFilePattern.indexOf("(") + 4));
		// Declaration of parameter fileArray[]
		String fileArray[] = fileNameORfilePath.split(delimiter);
		System.out.println(sourceFilePattern.substring(i,
				sourceFilePattern.indexOf("}") + 1) + " "
				+ fileArray[tokenValue - 1]);
		// return statement;
		return sourceFilePattern.replace(
				sourceFilePattern.substring(i,
						sourceFilePattern.indexOf("}") + 1),
				fileArray[tokenValue - 1]);
	}
}
