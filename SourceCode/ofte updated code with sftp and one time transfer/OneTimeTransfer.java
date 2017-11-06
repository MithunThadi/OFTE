package com.sftp.services;

import java.io.File;
import java.util.LinkedList;
import java.util.Map;

public class OneTimeTransfer {

	public void transfer(Map<String, String> metaDataMap) {
		System.out.println("enterde in one time transfer");
		TriggerPatternValidator triggerPatternValidator = new TriggerPatternValidator();
		LinkedList<String> filesList = new LinkedList<String>();
		String[] filesInDirectory;
		File file = new File(metaDataMap.get("sourceDirectory"));
		filesInDirectory = file.list();
		for (int i = 0; i < filesInDirectory.length; i++) {
			System.out.println((!filesInDirectory[i].equalsIgnoreCase("")) + " "
					+ (triggerPatternValidator.validateTriggerPattern(
							metaDataMap.get("triggerPattern"),
							filesInDirectory[i]) + " " + filesInDirectory[i]));
			// if loop to check the triggerPattern condition before adding
			// filesList
			if ((!filesInDirectory[i].equalsIgnoreCase(""))
					&& (triggerPatternValidator.validateTriggerPattern(
							metaDataMap.get("triggerPattern"),
							filesInDirectory[i]))) {
				// Adding filesInDirectory to filesList
				filesList.add(filesInDirectory[i]);

			}
		}

		// Creating an object for ProcessFiles class
		ProcessFiles processFiles = new ProcessFiles();
		// Invoking processFiles class to process the files in processFileList
		LinkedList<String> processFilesList = processFiles
				.processFileList(filesList, metaDataMap);
		// clear the processFilesList
		processFilesList.clear();
		System.out.println("files transfered sucessfully");
	}

}
