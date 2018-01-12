package com.ofte.file.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ofte.cassandra.services.CassandraInteracter;
import com.ofte.file.utility.TaskUtils;
import com.ofte.kafka.services.KafkaMapData;
import com.ofte.kafka.services.KafkaServerService;

import io.netty.handler.timeout.TimeoutException;
import kafka.common.InvalidConfigException;

public class ofteMonitor {
public static void main (String []args ) throws IOException {
	LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for TimedMonitor class
	Logger logger = Logger.getLogger(ofteMonitor.class.getName());
	// Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	// Declaration of parameter Map
	String monitorName1 = args[0];
	int previousListSize = 0;
	// Declaration of parameter file
	File file;
	// Declaration of parameter filesInDirectory
	String[] filesInDirectory;
	// Creating an object for SimpleDateFormat class
	SimpleDateFormat simpledateFormat = new SimpleDateFormat("ddHHmmss");
	// Creating an object for LinkedList class
	LinkedList<String> filesList = new LinkedList<String>();
	LinkedList<String> matchedFilesList = new LinkedList<String>();
	LinkedList<String> processFileList = new LinkedList<String>();
//	Map<String, String> transferMetaData = new HashMap<String, String>();
	// Creating an object for CassandraInteracter class
	CassandraInteracter cassandraInteracter = new CassandraInteracter();
	KafkaMapData kafkaMapData = new KafkaMapData();
	TaskUtils taskObject = new TaskUtils();
	try {
		
		// have to update the code to check delete status in cassandra
			try {
			String monitorStatus = cassandraInteracter.DBMonitorCheck(
					cassandraInteracter.connectCassandra(),
					monitorName1);
			// if (monitorStatus != null) {
			if (monitorStatus.equalsIgnoreCase("paused")) {
				return;
			}
			if (monitorStatus.equalsIgnoreCase("deleted")) {
				// if (timer != null) {
				System.out.println("entered in task delete");

				cassandraInteracter.deleteMonitor(
						cassandraInteracter.connectCassandra(),
						monitorName1);
				
				int delete = taskObject.deleteTask(monitorName1);
				if (delete == 0) {
					System.out.println(monitorName1+" deleted succesfully");
				}else {
					System.out.println(monitorName1+"deletion unsuccessfull");
				}

			}

		} catch (NoSuchFieldException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (SecurityException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		// Creating of Map object
		Map<String, String> metaDataMap = new HashMap<String, String>();
		// Declaration of parameter mapData and initialising
		String mapData = kafkaMapData
				.consume("Monitor_MetaData_" + monitorName1);
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
		// have to add the code to retrieve data from the cassandra db
		if (metaDataMap.isEmpty()) {
			System.out.println("retrieving data from cassandra database");
			String monitorMetaData = cassandraInteracter.getMonitorMetaData(
					cassandraInteracter.connectCassandra(), monitorName1);
			String[] cassandraMapDataArrays = monitorMetaData.split(",");
			// for loop to put the values into Map object
			for (int j = 0; j < cassandraMapDataArrays.length; j++) {
				metaDataMap.put(
						(cassandraMapDataArrays[j].substring(0,
								(cassandraMapDataArrays[j].indexOf("="))))
										.toString(),
						((cassandraMapDataArrays[j].substring(
								cassandraMapDataArrays[j].indexOf("=")
										+ 1))).toString());

			}
		}
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
		Timestamp currentTimeStamp = new Timestamp(
				System.currentTimeMillis());
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
						System.out.println("if loop: "
								+ (filesList.get(j)).toString());
						// Creating an object for File class and
						// initialising it with sourceDirectory by getting
						// values from metaDataMap
						File fileUnderProcess = new File(
								metaDataMap.get("sourceDirectory")
										+ filesInDirectory[i].toString());
						// Declaration of parameter lastStringModified and
						// initialising it with lastModified time
						String lastStringModified = simpledateFormat
								.format(fileUnderProcess.lastModified());
						// Declaration of parameter lastModified and
						// initialising it with lastStringModified time
						Long lastModified = Long
								.parseLong(lastStringModified);
						// if loop to check the condition lastModified
						if (((lastModified >= (currentTime - (Integer.parseInt(taskObject.calculatePoll(metaDataMap.get("pollUnits"), metaDataMap.get("pollInterval"))) * 60 * 1000)))
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
			System.out.println((!filesInDirectory[i].equalsIgnoreCase(""))
					+ " "
					+ (triggerPatternValidator.validateTriggerPattern(
							metaDataMap.get("triggerPattern"),
							filesInDirectory[i]) + " "
							+ filesInDirectory[i]));
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
		System.out.println(filesList.size());
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
					processFileList.add((filesList.get(i)).toString());
					System.out.println("tpv");
				}
			}
		}
		// Creating an object for ProcessFiles class
		ProcessFiles processFiles = new ProcessFiles();
		// Invoking processFiles class to process the files in
		// processFileList
		LinkedList<String> processFilesList = null;

		try {
			processFilesList = processFiles.processFileList(processFileList,
					metaDataMap);
		KafkaServerService kafkaServerService=new KafkaServerService();
		System.out.println("before shutdow0000000000000000000000000000000000000000000000000000000000000000");
		
		kafkaServerService.shutdown();
		System.out.println("afetr shutdow0000000000000000000000000000000000000000000000000000000000000000");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// clear the processFilesList
		processFilesList.clear();
	}
	// catching the exception for NoSuchMethodError
	catch (NoSuchMethodError noSuchMethodError) {
		noSuchMethodError
				.printStackTrace(new PrintWriter(log4jStringWriter));
		// logging the exception for NoSuchMethodError
		logger.error(loadProperties.getOFTEProperties().getProperty(
				"LOGGEREXCEPTION") + log4jStringWriter.toString());
	}
	// catching the exception for NoHostAvailableException
	catch (NoHostAvailableException noHostAvailableException) {
		noHostAvailableException
				.printStackTrace(new PrintWriter(log4jStringWriter));
		// logging the exception for NoHostAvailableException
		logger.error(loadProperties.getOFTEProperties().getProperty(
				"LOGGEREXCEPTION") + log4jStringWriter.toString());
	}
	// catching the exception for TimeoutException
	catch (TimeoutException timeoutException) {
		timeoutException
				.printStackTrace(new PrintWriter(log4jStringWriter));
		// logging the exception for TimeoutException
		logger.error(loadProperties.getOFTEProperties().getProperty(
				"LOGGEREXCEPTION") + log4jStringWriter.toString());
	}
	// catching the exception for
	// org.apache.kafka.common.errors.TimeoutException
	catch (org.apache.kafka.common.errors.TimeoutException apachecommonTimeoutException) {
		apachecommonTimeoutException
				.printStackTrace(new PrintWriter(log4jStringWriter));
		// logging the exception for
		// org.apache.kafka.common.errors.TimeoutException
		logger.error(loadProperties.getOFTEProperties().getProperty(
				"LOGGEREXCEPTION") + log4jStringWriter.toString());
	}
	// catching the exception for
	// org.jboss.netty.handler.timeout.TimeoutException
	catch (org.jboss.netty.handler.timeout.TimeoutException jbossTimeoutException) {
		jbossTimeoutException
				.printStackTrace(new PrintWriter(log4jStringWriter));
		// logging the exception for
		// org.jboss.netty.handler.timeout.TimeoutException
		logger.error(loadProperties.getOFTEProperties().getProperty(
				"LOGGEREXCEPTION") + log4jStringWriter.toString());
	}
	// catching the exception for InvalidConfigException
	catch (InvalidConfigException invalidConfigException) {
		invalidConfigException
				.printStackTrace(new PrintWriter(log4jStringWriter));
		// logging the exception for InvalidConfigExceptions
		logger.error(loadProperties.getOFTEProperties().getProperty(
				"LOGGEREXCEPTION") + log4jStringWriter.toString());
	}

}
}
