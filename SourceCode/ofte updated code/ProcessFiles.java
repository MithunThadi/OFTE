package com.ofte.services;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;
/**
 * 
 * Class Functionality:
 * 						The functionality of this class is to process the files
 * 
 * Methods:
 * 			public LinkedList<String> processFileList(LinkedList<String> processFileList,Map<String, String> metaDataMap)
 *
 */
public class ProcessFiles {
	//Creation of Map object
	Map<String, String> transferMetaData = new HashMap<String, String>();
	//Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	//Creating Logger object for TimedMonitor class
		Logger logger = Logger.getLogger(ProcessFiles.class.getName());
		//Creating an object for StringWriter class
		StringWriter log4jStringWriter = new StringWriter();

/**
 * This method is used to process the files
 * @param processFileList
 * @param metaDataMap
 * @return processFileList
 */
	public LinkedList<String> processFileList(
			LinkedList<String> processFileList,
			Map<String, String> metaDataMap) {
		//if loop to check the condition processFileList.size 
		if (processFileList.size() > 0) {
			//Creating an object for FilesProcessorService class
			FilesProcessorService filesProcessorService = new FilesProcessorService();
			//Creating an object for VariablesSubstitution class
			VariablesSubstitution variablesSubstitution = new VariablesSubstitution();
			//Creating an object for CassandraInteracter class
			CassandraInteracter cassandraInteracter = new CassandraInteracter();
			//for each loop to take the file in processFileList
			for (String file : processFileList) {
				//Declaration of parameters sourceFile and destinationFile and initialising it to null
				String sourceFile = null, destinationFile = null;
				System.out.println(file);
				//Declaration of parameters filePath and initialising it with sourceDirectory
				String filePath = metaDataMap.get("sourceDirectory") + "\\"
						+ file;
				//Inserting file and filePath to transferMetaData
				transferMetaData.put("FileName", file);
				transferMetaData.put("FilePath", filePath);
				//if loop to check the triggerPattern and sourcefilePattern condition
				if (metaDataMap.get("triggerPattern").equalsIgnoreCase(
						metaDataMap.get("sourcefilePattern"))) {
					sourceFile = filePath;
				} else if (metaDataMap.get("sourcefilePattern") != null) {
					sourceFile = variablesSubstitution.variableSubstitutor(
							transferMetaData,
							metaDataMap.get("sourcefilePattern"));
				}
				//Declaration of parameters targetFile and initialising it with destinationDirectory
				String targetFile = metaDataMap.get("destinationDirectory")
						+ sourceFile.substring(sourceFile.lastIndexOf("\\"));
				//if loop  to check the condition destinationDirectory
				if (metaDataMap.get("destinationDirectory") != null) {
					destinationFile = targetFile;
				} else if (metaDataMap.get("destinationFile") != null) {
					destinationFile = variablesSubstitution.variableSubstitutor(
							transferMetaData,
							metaDataMap.get("destinationFile"));
				}
				// Creating an object for UniqueID class
				UniqueID uniqueIDTest = new UniqueID();
				//Declaration of parameters transferId and initialising it with generateUniqueID
				String transferId = uniqueIDTest.generateUniqueID();
				System.out.println(transferId);
				//Inserting transferId, sourceFile and destinationFile to transferMetaData
				transferMetaData.put("transferId", transferId);
				transferMetaData.put("sourceFileName", sourceFile);
				transferMetaData.put("destinationFile", destinationFile);
				//Updating the database based on monitorName
				cassandraInteracter.started(cassandraInteracter.connectCassandra(),
						metaDataMap.get("monitorName"));
				try {
					// Creating an object for KafkaSecondLayer class
					KafkaSecondLayer kafkaSecondLayer = new KafkaSecondLayer();
					//Publishing the monitor table data
					kafkaSecondLayer.publish(loadProperties.getOFTEProperties().getProperty("TOPICNAME"),
							metaDataMap.get("monitorName"),
							cassandraInteracter.kafkaSecondCheckMonitor(
									cassandraInteracter.connectCassandra(),
									metaDataMap.get("monitorName")));
				} 
				//catching the exception for NoSuchFieldException
				catch (NoSuchFieldException noSuchFieldException) {
					noSuchFieldException.printStackTrace(new PrintWriter(log4jStringWriter));
					//logging the exception for NoSuchFieldException
					logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());

				} 
				//catching the exception for SecurityException
				catch (SecurityException securityException) {
					securityException.printStackTrace(new PrintWriter(log4jStringWriter));
					//logging the exception for SecurityException
					logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
				}
				//Updating the database
				cassandraInteracter.transferDetails(cassandraInteracter.connectCassandra(),
						metaDataMap, transferMetaData);
				//Updating the database 
				cassandraInteracter.transferEventDetails(
						cassandraInteracter.connectCassandra(), metaDataMap,
						transferMetaData);
				//Invoking FilesProcessorService class
				filesProcessorService.getMessages(filePath, metaDataMap,
						transferMetaData);
				System.out.println("fileProcessor releasing");
				// lock.unlock();
			}
		}
		//	return statement
		return processFileList;

	}

}
