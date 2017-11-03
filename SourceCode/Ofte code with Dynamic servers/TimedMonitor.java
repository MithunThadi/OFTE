package com.ofte.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.InvalidACLException;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import io.netty.handler.timeout.TimeoutException;
import kafka.common.InvalidConfigException;
/**
 * 
 * Class Functionality:
 * 						The main functionality of this class is depending upon the user command it watches the directory for a particular period of time and publishing the files
 * 
 * Methods:
 * 			public void timerAccess(Map<String, String> metaDataMap)
 * 			public void run()
 *
 */
public class TimedMonitor extends TimerTask {
	//Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	//Creating Logger object for TimedMonitor class
	Logger logger = Logger.getLogger(TimedMonitor.class.getName());
	//Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	//Declaration of parameter Map
	static String monitorName1;
	//Creating an object for SimpleDateFormat class
	SimpleDateFormat simpledateFormat = new SimpleDateFormat("ddHHmmss");
	//Declaration of parameter previousListSize
	int previousListSize = 0;
	//Declaration of parameter file
	File file;
	//Declaration of parameter filesInDirectory
	String[] filesInDirectory;
	//Creating an object for LinkedList class
	LinkedList<String> filesList = new LinkedList<String>();
	LinkedList<String> matchedFilesList = new LinkedList<String>();
	LinkedList<String> processFileList = new LinkedList<String>();
	//Declaration of parameter timer
	Timer timer;
	//Declaration of parameter pollTime
	long pollTime;
	//Creation of Map object
	Map<String, String> transferMetaData = new HashMap<String, String>();
	//Creating an object for CassandraInteracter class
	CassandraInteracter cassandraInteracter=new CassandraInteracter();
	KafkaMapData kafkaMapData = new KafkaMapData();

	/**
	 * This method is used to calculate the poll time by using poll Interval and poll units
	 * @param pollUnits 
	 * @param interval 
	 * @param monitorName 
	 */
	public void timerAccess(String monitorName, String interval, String pollUnits) {
		try {
			//Initialising monitorName1 with monitorName
			monitorName1 = monitorName;
			//Creating an object for Timer class
			Timer timer = new Timer();
			//Initialising pollInterval by getting the pollInterval from metaDataMap
			int pollInterval = Integer.parseInt(interval);
			//Initialising pollTime to zero
			long pollTime = 0;
			switch (pollUnits) {
			//Depending upon the case we are setting the values into metaDataMap
			case "minutes":
				pollTime = pollInterval * 60 * 1000;
				break;
			case "seconds":
				pollTime = pollInterval * 1000;
				break;
			case "hours":
				pollTime = pollInterval * 60 * 60 * 1000;
				break;
			case "days":
				pollTime = pollInterval * 60 * 60 * 24 * 1000;
				break;
			}
			//Watching the directory at scheduled time interval
			timer.scheduleAtFixedRate(new TimedMonitor(), 1000, pollTime);
		} 
		//catching the exception for NumberFormatException
		catch (NumberFormatException numberFormatException) {
			numberFormatException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NumberFormatException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
	@Override
	/**
	 * 
	 */
	public void run() {
		try {
			//Creating of Map object 
			Map<String, String> metaDataMap = new HashMap<String, String>();
			//Declaration of parameter mapData and initialising
			String mapData = kafkaMapData.consume("Monitor_MetaData_"+monitorName1);
			//Declaration of parameter mapDataArrays and initialising it with map values
			String[] mapDataArrays = mapData.split(",");
			//for loop to put the values into Map object 
            for (int j = 0; j < mapDataArrays.length; j++) {
                metaDataMap.put(
                        (mapDataArrays[j].substring(0,
                                (mapDataArrays[j].indexOf("=")))).toString(),
                        ((mapDataArrays[j]
                                .substring(mapDataArrays[j].indexOf("=") + 1)))
                                        .toString());

            }
			//Creating an object for File class and initialising it with sourceDirectory by getting the values from metaDataMap
			file = new File(metaDataMap.get("sourceDirectory"));
			System.out.println("Timer created for::" + file);
			//Declaration of parameter numberOfFiles and initialising it with file.listFiles().length
			int numberOfFiles = file.listFiles().length;
			System.out.println(numberOfFiles);
			//Initialising filesInDirectory with file.list()
			filesInDirectory = file.list();
			//Initialising previousListSize with filesList.size()
			previousListSize = filesList.size();
			//Creating an object for Timestamp class
			Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
			//Declaration of parameter currentTime and initialising it with currentTimeStamp
			Long currentTime = Long.parseLong(simpledateFormat.format(currentTimeStamp));
			//if loop to check the condition previousListSize not equals to zero
			if (previousListSize != 0) {
				//for loop to add the file in matchedFilesList 
				for (int i = 0; i < numberOfFiles; i++) {
					System.out.println("list size is " + previousListSize);
					//for loop to add the files in matchedFilesList
					for (int j = 0; j < previousListSize; j++) {
						//if loop to check the condition filesList equals to filesInDirectory
						if ((filesList.get(j)).toString().equals(filesInDirectory[i].toString())) {
							System.out.println("if loop: " + (filesList.get(j)).toString());
							//Creating an object for File class and initialising it with sourceDirectory by getting values from metaDataMap
							File file = new File(metaDataMap.get("sourceDirectory") + filesInDirectory[i].toString());
							//Declaration of parameter lastStringModified and initialising it with lastModified time
							String lastStringModified = simpledateFormat.format(file.lastModified());
							//Declaration of parameter lastModified and initialising it with lastStringModified time
							Long lastModified = Long.parseLong(lastStringModified);
							//if loop to check the condition lastModified
							if (((lastModified >= (currentTime - pollTime)) && (lastModified < currentTime))) {
								continue;
							} else {
								matchedFilesList.add(filesInDirectory[i]);
							}
							//Replacing filesInDirectory array with no value
							filesInDirectory[i] = "";
						}
					}
				}
			}
			//clear filesList
			filesList.clear();
			//adding matchedFilesList to filesList
			filesList.addAll(matchedFilesList);
			//clear matchedFilesList
			matchedFilesList.clear();
			//Initialising previousListSize with filesList.size
			previousListSize = filesList.size();
			System.out.println(filesInDirectory.length);
			//Creating an object for TriggerPatternValidator class
			TriggerPatternValidator triggerPatternValidator=new TriggerPatternValidator();
			//for loop to add filesInDirectory to filesList
			for (int i = 0; i < filesInDirectory.length; i++) {
				System.out.println((!filesInDirectory[i].equalsIgnoreCase(""))+ " "+  (triggerPatternValidator
						.validateTriggerPattern(metaDataMap.get("triggerPattern"), filesInDirectory[i]) + " " + filesInDirectory[i]));
				//if loop to check the triggerPattern condition before adding filesList
				if ((!filesInDirectory[i].equalsIgnoreCase("")) && (triggerPatternValidator
						.validateTriggerPattern(metaDataMap.get("triggerPattern"), filesInDirectory[i]))) {
					//Adding filesInDirectory to filesList
					filesList.add(filesInDirectory[i]);
					System.out.println("tpv");
				}
			}
			System.out.println(filesList.size());
			//if loop  to check previousListSize and filesListsize 
			if (previousListSize < filesList.size()) {
				System.out.println(filesList);
				//Initialising the parameter count
				int count = (filesList.size() - (filesList.size() - previousListSize));
				//for loop  to check previousListSize and filesListsize to add the new files in processFileList
				for (int i = count; i < filesList.size(); i++) {
					System.out.println(filesList.get(i));
					//if loop to check the condition triggerPatternValidator and adding processFileList
					if (triggerPatternValidator.validateTriggerPattern(metaDataMap.get("triggerPattern"),
							filesList.get(i).toString())) {
						//Adding filesList to processFileList 
						processFileList.add((filesList.get(i)).toString());
						System.out.println(processFileList);
						System.out.println("tpv");
					}
				}
			}
			//Creating an object for ProcessFiles class
			ProcessFiles processFiles = new ProcessFiles();
			System.out.println(metaDataMap);
			System.out.println(processFileList);
			//Invoking processFiles class to process the files in processFileList
			LinkedList<String> processFilesList = processFiles.processFileList(processFileList, metaDataMap);
			//clear the processFilesList
			processFilesList.clear();
		} 
		//catching the exception for NoSuchMethodError
		catch (NoSuchMethodError noSuchMethodError) {
			noSuchMethodError.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoSuchMethodError
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION")+ log4jStringWriter.toString());
		}
		//catching the exception for TimeoutException
		catch (TimeoutException timeoutException) {
			timeoutException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for TimeoutException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for org.apache.kafka.common.errors.TimeoutException
		catch (org.apache.kafka.common.errors.TimeoutException apachecommonTimeoutException) {
			apachecommonTimeoutException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for org.apache.kafka.common.errors.TimeoutException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for org.jboss.netty.handler.timeout.TimeoutException
		catch (org.jboss.netty.handler.timeout.TimeoutException jbossTimeoutException) {
			jbossTimeoutException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for org.jboss.netty.handler.timeout.TimeoutException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for InvalidConfigException
		catch (InvalidConfigException invalidConfigException) {
			invalidConfigException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for InvalidConfigExceptions
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}