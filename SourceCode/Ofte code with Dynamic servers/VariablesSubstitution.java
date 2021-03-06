package com.ofte.services;

import java.util.Map;
/**
 * 
 * Class Functionality:
 * 						This class is used to replace the file extension part				
 * Methods:
 * 			public String variableSubstitutor(Map<String, String> transferMetaDataMap, String sourceFilePattern)
 * 			public String replacer(String sourceFilePattern, String fileNameORfilePath, int i)
 *
 */
public class VariablesSubstitution {
	/**
	 * This method returns the string based on the delimiter and token value given in the sourcefilepattern.
	 * @param transferMetaDataMap
	 * @param sourceFilePattern
	 * @return sourceFilePattern
	 */

	public String variableSubstitutor(Map<String, String> transferMetaDataMap, String sourceFilePattern){ 
		
		//Declaration of parameter FileName and initialising it by getting FileName from transferMetaDataMap
				String FileName = transferMetaDataMap.get("FileName");
				//Declaration of parameter FilePath and initialising it by getting FilePath from transferMetaDataMap
				String FilePath = transferMetaDataMap.get("FilePath");
				//for loop to increment i value until it is less than sourceFilePattern.length()
				System.out.println("entered vs");
				for (int i = 0; i < sourceFilePattern.length(); i++) {
					//if loop to check sourceFilePattern.charAt(i) is equal to #
					if (sourceFilePattern.charAt(i) == '#') {
						System.out.println("entered for and if");
						//switch case for selecting FileName or FilePath
						switch (sourceFilePattern.substring(i + 2, sourceFilePattern.indexOf("("))) {
						case "FileName":
							sourceFilePattern = replacer(sourceFilePattern, FileName, i);
							System.out.println(sourceFilePattern);
							break;
						case "FilePath":
							System.out.println("entered FilePath");
							sourceFilePattern = replacer(sourceFilePattern, FilePath, i);
							break;
						default:
							break;
						}
					}
				}
				System.out.println(sourceFilePattern);
				//return statement
				return sourceFilePattern;				
			}
		/**
		 * 
		 * @param sourceFilePattern
		 * @param fileNameORfilePath
		 * @param i
		 * @return
		 */
			public String replacer(String sourceFilePattern, String fileNameORfilePath, int i) {
				//Declaration of parameter delimiter
				System.out.println("entered replacer");
				String delimiter = sourceFilePattern.substring(sourceFilePattern.indexOf("(") + 1, sourceFilePattern.indexOf("(") + 2);
				//if loop to check delimiter
				if (delimiter.equalsIgnoreCase(".")) {
					System.out.println("setting delimiter");
					delimiter = "\\.";
				} else if (delimiter.equalsIgnoreCase("\\")) {
					delimiter = "\\\\";
				}
				//Declaring and initialising tokenValue for setting variable
				int tokenValue = Integer.parseInt(sourceFilePattern.substring(sourceFilePattern.indexOf("(") + 3, sourceFilePattern.indexOf("(") + 4));
				//Declaration of parameter fileArray[]
				String fileArray[] = fileNameORfilePath.split(delimiter);
				System.out.println(sourceFilePattern.substring(i, sourceFilePattern.indexOf("}") + 1) + " " + fileArray[tokenValue - 1]);
				//return statement;
				return sourceFilePattern.replace(sourceFilePattern.substring(i, sourceFilePattern.indexOf("}") + 1), fileArray[tokenValue - 1]);
			}
		}
