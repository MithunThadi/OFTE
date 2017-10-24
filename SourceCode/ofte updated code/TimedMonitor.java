package com.ofte.services;

import java.io.File;
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
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import io.netty.handler.timeout.TimeoutException;
import kafka.common.InvalidConfigException;
/**
 * 
 * Class Functionality:
 * 						The main functionality of this class is depending upon the user command it watches the directory for a particular period of time and publishing the files
 * 
 * Methods:
 * 			public TimedMonitor(Timer timer,Long pollTime)
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
	static Map<String, String> metaDataMap1;
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
	/**
	 * 
	 * @param timer
	 * @param pollTime
	 */
	public TimedMonitor(Timer timer,Long pollTime) {
		this.timer = timer;
		this.pollTime = pollTime;
	}
	/**
	 * 
	 * @param metaDataMap
	 */
	public void timerAccess(Map<String, String> metaDataMap) {
		try {
			metaDataMap1 = metaDataMap;
			//Creating an object for Timer class
			Timer timer = new Timer();
			//Initialising pollInterval by getting the pollInterval from metaDataMap
			int pollInterval = Integer.parseInt(metaDataMap.get("pollInterval"));
			//Initialising pollUnits by getting the pollUnits from metaDataMap
			String pollUnits = metaDataMap.get("pollUnits");
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
			timer.scheduleAtFixedRate(new TimedMonitor(timer, pollTime), 1000, pollTime);
		} 
		//catching the exception for NumberFormatException
		catch (NumberFormatException numberFormatException) {
			numberFormatException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NumberFormatException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
	@Override
	public void run() {
		try {
			//Creating an object for File class
			file = new File(metaDataMap1.get("sourceDirectory"));
			System.out.println("Timer created for::" + file);
			//
			int numberOfFiles = file.listFiles().length;
			System.out.println(numberOfFiles);
			//
			filesInDirectory = file.list();
			//
			previousListSize = filesList.size();
			Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
			Long currentTime = Long.parseLong(simpledateFormat.format(currentTimeStamp));
			if (previousListSize != 0) {
				for (int i = 0; i < numberOfFiles; i++) {
					System.out.println("list size is " + previousListSize);
					for (int j = 0; j < previousListSize; j++) {
						if ((filesList.get(j)).toString().equals(filesInDirectory[i].toString())) {
							System.out.println("if loop: " + (filesList.get(j)).toString());
							File file = new File(metaDataMap1.get("sourceDirectory") + filesInDirectory[i].toString());
							String lastStringModified = simpledateFormat.format(file.lastModified());
							Long lastModified = Long.parseLong(lastStringModified);
							if (((lastModified >= (currentTime - pollTime)) && (lastModified < currentTime))) {
								continue;
							} else {
								matchedFilesList.add(filesInDirectory[i]);
							}
							filesInDirectory[i] = "";
						}
					}
				}
			}
			filesList.clear();
			filesList.addAll(matchedFilesList);
			matchedFilesList.clear();
			previousListSize = filesList.size();
			System.out.println(filesInDirectory.length);
			TriggerPatternValidator triggerPatternValidator=new TriggerPatternValidator();
			for (int i = 0; i < filesInDirectory.length; i++) {
				System.out.println((!filesInDirectory[i].equalsIgnoreCase(""))+ " "+  (triggerPatternValidator
						.validateTriggerPattern(metaDataMap1.get("triggerPattern"), filesInDirectory[i]) + " " + filesInDirectory[i]));
				if ((!filesInDirectory[i].equalsIgnoreCase("")) && (triggerPatternValidator
						.validateTriggerPattern(metaDataMap1.get("triggerPattern"), filesInDirectory[i]))) {
					filesList.add(filesInDirectory[i]);
					System.out.println("tpv");
				}
			}
			System.out.println(filesList.size());
			if (previousListSize < filesList.size()) {
				System.out.println(filesList);
				int count = (filesList.size() - (filesList.size() - previousListSize));
				for (int i = count; i < filesList.size(); i++) {
					System.out.println(filesList.get(i));
					if (triggerPatternValidator.validateTriggerPattern(metaDataMap1.get("triggerPattern"),
							filesList.get(i).toString())) {
						processFileList.add((filesList.get(i)).toString());
						System.out.println("tpv");
					}
				}
			}
			FilesProcessorService filesProcessorService = new FilesProcessorService();
			VariablesSubstitution variablesSubstitution=new VariablesSubstitution();
			
			if (processFileList.size() > 0) {
				for (String file : processFileList) {
					String sourceFile=null,destinationFile=null;
					System.out.println(file);
					String filePath = metaDataMap1.get("sourceDirectory") + "\\" + file;
					transferMetaData.put("FileName", file);
					transferMetaData.put("FilePath", filePath);
					if(metaDataMap1.get("triggerPattern").equalsIgnoreCase(metaDataMap1.get("sourcefilePattern"))) {
						sourceFile=filePath;
					}else if(metaDataMap1.get("sourcefilePattern")!=null) {
						sourceFile=variablesSubstitution.variableSubstitutor(transferMetaData, metaDataMap1.get("sourcefilePattern"));
					}
					String targetFile = metaDataMap1.get("destinationDirectory") + sourceFile.substring(sourceFile.lastIndexOf("\\"));
					if(metaDataMap1.get("destinationDirectory")!=null) {
						destinationFile=targetFile;
					}else if(metaDataMap1.get("destinationFile")!=null) {
						destinationFile= variablesSubstitution.variableSubstitutor(transferMetaData, metaDataMap1.get("destinationFile"));
					}
					UniqueID uniqueID=new UniqueID();
					String transferId = uniqueID.generateUniqueID();
					System.out.println(transferId);
					transferMetaData.put("transferId", transferId);
					transferMetaData.put("sourceFileName", sourceFile);
					transferMetaData.put("destinationFile", destinationFile);
					cassandraInteracter.started(cassandraInteracter.connectCassandra(), metaDataMap1.get("monitorName"));
					try {
						KafkaSecondLayer kafkaSecondLayer=new KafkaSecondLayer();
						kafkaSecondLayer.publish(loadProperties.getOFTEProperties().getProperty("TOPICNAME"), metaDataMap1.get("monitorName"), cassandraInteracter.kafkaSecondCheckMonitor(cassandraInteracter.connectCassandra(),metaDataMap1.get("monitorName")));
					} catch (NoSuchFieldException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
					cassandraInteracter.transferDetails(cassandraInteracter.connectCassandra(), metaDataMap1, transferMetaData);
					
					cassandraInteracter.transferEventDetails(cassandraInteracter.connectCassandra(), metaDataMap1, transferMetaData);
					filesProcessorService.getMessages(filePath, metaDataMap1, transferMetaData);
					System.out.println("fileProcessor releasing");
				}
			}
			processFileList.clear();
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
		} 
	}
}