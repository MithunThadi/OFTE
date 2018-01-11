package com.ofte.file.services;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.ofte.kafka.services.KafkaMapData;

public class LocalSingleTimeTrigger {
	KafkaMapData kafkaMapData = new KafkaMapData();
	long pollTime;

	SimpleDateFormat simpledateFormat = new SimpleDateFormat("ddHHmmss");
	// Declaration of parameter previousListSize
	int previousListSize = 0;
	// Declaration of parameter file
	File file;
	// Declaration of parameter filesInDirectory
	String[] filesInDirectory;
	// Creating an object for LinkedList class
	LinkedList<String> filesList = new LinkedList<String>();
	LinkedList<String> matchedFilesList = new LinkedList<String>();
	LinkedList<String> filesToUpload = new LinkedList<String>();
	public LinkedList<String> singleTimeTrigger(String schedulerName,
			long pollTime2) {
		// Creating of Map object
		Map<String, String> metaDataMap = new HashMap<String, String>();
		// Declaration of parameter mapData and initialising
		String mapData = kafkaMapData
				.consume("Scheduler_MetaData_" + schedulerName);
		// Declaration of parameter mapDataArrays and initialising it with
		// map values
		String[] mapDataArrays = mapData.split(",");
		// for loop to put the values into Map object
		for (int j = 0; j < mapDataArrays.length; j++) {
			metaDataMap.put(
					(mapDataArrays[j].substring(0,
							(mapDataArrays[j].indexOf("=")))).toString(),
					((mapDataArrays[j]
							.substring(mapDataArrays[j].indexOf("=") + 1)))
									.toString());

		}
		// Creating an object for File class and initialising it with
		// sourceDirectory by getting the values from metaDataMap
		file = new File(metaDataMap.get("sourceDirectory"));
		System.out.println("Timer created for::" + file);
		// Declaration of parameter numberOfFiles and initialising it with
		// file.listFiles().length
		int numberOfFiles = file.listFiles().length;
		System.out.println(numberOfFiles);
		// Initialising filesInDirectory with file.list()
		filesInDirectory = file.list();
		// Initialising previousListSize with filesList.size()
		previousListSize = filesList.size();
		// Creating an object for Timestamp class
		Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
		// Declaration of parameter currentTime and initialising it with
		// currentTimeStamp
		Long currentTime = Long
				.parseLong(simpledateFormat.format(currentTimeStamp));
		// if loop to check the condition previousListSize not equals to
		// zero
		if (previousListSize != 0) {
			// for loop to add the file in matchedFilesList
			for (int i = 0; i < numberOfFiles; i++) {
				System.out.println("list size is " + previousListSize);
				// for loop to add the files in matchedFilesList
				for (int j = 0; j < previousListSize; j++) {
					// if loop to check the condition filesList equals to
					// filesInDirectory
					if ((filesList.get(j)).toString()
							.equals(filesInDirectory[i].toString())) {
						System.out.println(
								"if loop: " + (filesList.get(j)).toString());
						// Creating an object for File class and
						// initialising it with sourceDirectory by getting
						// values from metaDataMap
						File file = new File(metaDataMap.get("sourceDirectory")
								+ filesInDirectory[i].toString());
						// Declaration of parameter lastStringModified and
						// initialising it with lastModified time
						String lastStringModified = simpledateFormat
								.format(file.lastModified());
						// Declaration of parameter lastModified and
						// initialising it with lastStringModified time
						Long lastModified = Long.parseLong(lastStringModified);
						// if loop to check the condition lastModified
						if (((lastModified >= (currentTime - pollTime))
								&& (lastModified < currentTime))) {
							continue;
						} else {
							matchedFilesList.add(filesInDirectory[i]);
						}
						// Replacing filesInDirectory array with no value
						filesInDirectory[i] = "";
					}
				}
			}
		}
		// clear filesList
		filesList.clear();
		// adding matchedFilesList to filesList
		filesList.addAll(matchedFilesList);
		// clear matchedFilesList
		matchedFilesList.clear();
		// Initialising previousListSize with filesList.size
		previousListSize = filesList.size();
		System.out.println(filesInDirectory.length);
		// Creating an object for TriggerPatternValidator class
		TriggerPatternValidator triggerPatternValidator = new TriggerPatternValidator();
		// for loop to add filesInDirectory to filesList
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
				System.out.println("tpv");
			}
		}
		System.out.println(filesList.size()
				+ " if loop  to check previousListSize and filesListsize");
		// if loop to check previousListSize and filesListsize
		if (previousListSize < filesList.size()) {
			System.out.println(filesList);
			// Initialising the parameter count
			int count = (filesList.size()
					- (filesList.size() - previousListSize));
			// for loop to check previousListSize and filesListsize to add
			// the new files in processFileList
			for (int i = count; i < filesList.size(); i++) {
				System.out.println(filesList.get(i));
				// if loop to check the condition triggerPatternValidator
				// and adding processFileList
				if (triggerPatternValidator.validateTriggerPattern(
						metaDataMap.get("triggerPattern"),
						filesList.get(i).toString())) {
					// Adding filesList to processFileList
					filesToUpload.add((filesList.get(i)).toString());
					System.out.println("tpv");
				}
			}
		}
		return filesToUpload;
	}

}
