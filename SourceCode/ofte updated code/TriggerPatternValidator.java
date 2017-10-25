package com.ofte.services;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * 
 * Class functionality:
 * 						The main functionality of this class is based on the trigger condition given by the user command, the files gets triggered and then processed 
 * 
 * Methods:
 * 			public boolean validateTriggerPattern(String triggerPattern, String fileName)
 * 	
 *
 */
public class TriggerPatternValidator {
	/**
	 * This method returns the boolean value either true or false based on the condition that the filename is matched with the given triggerpattern condition
	 * @param triggerPattern
	 * @param fileName
	 * @return validatedResult
	 */
	public boolean validateTriggerPattern(String triggerPattern, String fileName) {
		//Declaring and initialising parameter validatedResult to true
		boolean validatedResult = true;
		//Declaration of parameter fileNameLength
		int fileNameLength = fileName.length();
		//Declaration of parameter index
		int index;
		//if loop to check trigger pattern contains * or ?
		if (triggerPattern.contains("*") || triggerPattern.contains("?")) {
			//if loop to check the condition if triggerPattern contains * and setting validatedResult to false
			if (triggerPattern.contains("*")) {
				index = triggerPattern.lastIndexOf("*");
				if (!fileName.contains(triggerPattern.substring(0, index))) {
					System.out.println("false 1");
					validatedResult = false;
				}
				if (!fileName.contains(triggerPattern.substring(index + 1))) {
					System.out.println("false 2");
					validatedResult = false;
				}
			}
			//if loop to check the condition if triggerPattern contains ? and setting validatedResult to false
			if (triggerPattern.contains("?")) {
				index = triggerPattern.lastIndexOf("?");
				triggerPattern.indexOf("?");
				if (!fileName.contains(triggerPattern.substring(0, triggerPattern.indexOf("?")))) {
					validatedResult = false;
					System.out.println("false 3");
				}
				if (!fileName.contains(triggerPattern.substring(index + 1))) {
					validatedResult = false;
					System.out.println("false 4");
				}
				if (!(triggerPattern.length() == fileNameLength)) {
					validatedResult = false;
					System.out.println("false 5");
				}
			}
		} else {
			Pattern pattern = Pattern.compile(triggerPattern);
			//Declaration of parameter matcher and initialising it to null
			Matcher matcher = null;
			//Declaration of parameter length and initialising it to zero
			int length = 0;
			// while loop to check the condition length less than fileNameLength
			while (length < fileNameLength) {
				//Declaration of parameter characterInFileName
				char characterInFileName = fileName.charAt(length);
				//Declaration of parameter character
				String character = Character.toString(characterInFileName);
				matcher = pattern.matcher(character);
				validatedResult = matcher.find();
				//if loop to check validatedResult
				if (validatedResult == false) {
					break;
				}
				//incrementing length
				length++;
			}
		}
		//return statement
		return validatedResult;
	}
}