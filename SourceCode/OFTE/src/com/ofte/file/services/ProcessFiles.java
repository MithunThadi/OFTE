package com.ofte.file.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;

import com.ofte.cassandra.services.CassandraInteracter;
import com.ofte.kafka.services.KafkaSecondLayer;
import com.ofte.kafka.services.KafkaServerService;

import kafka.utils.ZkUtils;

/**
 * 
 * Class Functionality: The functionality of this class is to process the files
 * 
 * Methods: public LinkedList<String> processFileList(LinkedList<String>
 * processFileList,Map<String, String> metaDataMap)
 *
 */
public class ProcessFiles {
	// Creation of Map object
	// Map<String, String> transferMetaData = new HashMap<String, String>();
	// Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for TimedMonitor class
	Logger logger = Logger.getLogger(ProcessFiles.class.getName());
	// Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	ZkClient zkClient;
	ZkUtils zkUtils;
	String transferId = null;
	// KafkaServerService kafkaServerService = new KafkaServerService();

	/**
	 * This method is used to process the files
	 * 
	 * @param processFileList
	 * @param metaDataMap
	 * @return processFileList
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	@SuppressWarnings("static-access")
	public synchronized LinkedList<String> processFileList(LinkedList<String> processFileList,
			Map<String, String> metaDataMap) throws IOException, InterruptedException, NoSuchMethodException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		// if loop to check the condition processFileList.size
		if (processFileList.size() > 0) {
			// Creating an object for FilesProcessorService class
			// FilesProcessorService filesProcessorService = new FilesProcessorService();
			// Creating an object for VariablesSubstitution class
			VariablesSubstitution variablesSubstitution = new VariablesSubstitution();
			KafkaServerService kafkaServerService = new KafkaServerService();
			// Creating an object for CassandraInteracter class
			CassandraInteracter cassandraInteracter = new CassandraInteracter();

			cassandraInteracter.started(cassandraInteracter.connectCassandra(), metaDataMap.get("monitorName"),
					metaDataMap.get("pollTime"));
			ExecutorService pool = Executors.newFixedThreadPool(processFileList.size());

			kafkaServerService.setBROKER_PORT(0);
			kafkaServerService.setId(0);
			kafkaServerService.setZkPort(0);
			zkClient = kafkaServerService.setupEmbeddedZooKeeper();
			kafkaServerService.setupEmbeddedKafkaHugeServer();
			zkUtils = kafkaServerService.accessZkUtils();

			// for each loop to take the file in processFileList
			for (String file : processFileList) {
				// preDst
				String linuxDelimeter = "/";
				String windowsDelimeter = "\\";
				String destinationFilePath = null;
				if (System.getProperty("os.name").contains("Windows")) {
					destinationFilePath = metaDataMap.get("destinationDirectory") + windowsDelimeter + file;
				} else if (System.getProperty("os.name").contains("Linux")) {
					destinationFilePath = metaDataMap.get("destinationDirectory") + linuxDelimeter + file;
				}
				System.out.println(destinationFilePath + " destination file check");

				File destinationFileCheck = new File(destinationFilePath);

				if (destinationFileCheck.exists()
						&& metaDataMap.get("destinationExists").toString().equalsIgnoreCase("overWrite")) {
					System.out.println("enterd in over write");
					// destinationFileCheck.delete();
					FileUtils.forceDelete(destinationFileCheck);
				} else if (destinationFileCheck.exists()
						&& metaDataMap.get("destinationExists").toString().equalsIgnoreCase("error")) {
					try {
						System.out.println("enterd in error");
						// have to update in db as file failed

						throw new Exception(
								"file already existed in target path please specify parameter as overwrite");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						System.out.println("file existed already");
						e.printStackTrace();
					}
				}
				if (!destinationFileCheck.exists()) {

					// Declaration of parameters filePath and
					// initialising
					// it
					// with
					// sourceDirectory
					String filePath = null;
					if (System.getProperty("os.name").contains("Windows")) {
						filePath = metaDataMap.get("sourceDirectory") + windowsDelimeter + file;
					} else if (System.getProperty("os.name").contains("Linux")) {
						filePath = metaDataMap.get("sourceDirectory") + linuxDelimeter + file;
					}

					Map<String, String> transferMetaData = new HashMap<String, String>();

					// Declaration of parameters sourceFile and
					// destinationFile
					// and
					// initialising it to null
					String sourceFile = null, destinationFile = null;
					System.out.println(file);

					// Inserting file and filePath to transferMetaData
					transferMetaData.put("FileName", file);
					transferMetaData.put("FilePath", filePath);
					// if loop to check the triggerPattern and
					// sourcefilePattern
					// condition
					if (metaDataMap.get("triggerPattern").equalsIgnoreCase(metaDataMap.get("sourcefilePattern"))) {
						sourceFile = transferMetaData.get("FilePath");

					} else if (metaDataMap.get("sourcefilePattern") != null) {
						if (System.getProperty("os.name").contains("Windows")) {
							sourceFile = metaDataMap.get("sourceDirectory") + windowsDelimeter
									+ variablesSubstitution
											.variableSubstitutor(transferMetaData, metaDataMap.get("sourcefilePattern"))
											.replace("*", file.substring(0, file.lastIndexOf(".")));
							System.out.println("source file in else if " + sourceFile);
						} else if (System.getProperty("os.name").contains("Linux")) {
							sourceFile = metaDataMap.get("sourceDirectory") + linuxDelimeter
									+ variablesSubstitution
											.variableSubstitutor(transferMetaData, metaDataMap.get("sourcefilePattern"))
											.replace("*", file.substring(0, file.lastIndexOf(".")));
							System.out.println("source file in else if " + sourceFile);
						}

					}
					// Declaration of parameters targetFile and
					// initialising
					// it
					// with
					// destinationDirectory

					// if loop to check the condition
					// destinationDirectory
					if (metaDataMap.get("destinationDirectory") != null) {
						if (metaDataMap.get("destinationFilePattern").toString()
								.equalsIgnoreCase(metaDataMap.get("sourcefilePattern"))) {
							if (System.getProperty("os.name").contains("Windows")) {
								destinationFile = metaDataMap.get("destinationDirectory").toString()
										.concat(sourceFile.substring(sourceFile.lastIndexOf(windowsDelimeter)));
								System.out.println("in if loop " + destinationFile);
							} else if (System.getProperty("os.name").contains("Linux")) {
								destinationFile = metaDataMap.get("destinationDirectory").toString()
										.concat(sourceFile.substring(sourceFile.lastIndexOf(linuxDelimeter)));
								System.out.println("in if loop " + destinationFile);
							}
						} else {
							if (System.getProperty("os.name").contains("Windows")) {
								destinationFile = metaDataMap.get("destinationDirectory").concat(windowsDelimeter)
										.concat(variablesSubstitution
												.variableSubstitutor(transferMetaData,
														metaDataMap.get("destinationFilePattern"))
												.replace("*",
														sourceFile.substring(
																sourceFile.lastIndexOf(windowsDelimeter) + 1,
																sourceFile.lastIndexOf("."))));
								System.out.println("in else loop " + destinationFile);
							} else if (System.getProperty("os.name").contains("Linux")) {
								destinationFile = metaDataMap.get("destinationDirectory").concat(linuxDelimeter)
										.concat(variablesSubstitution
												.variableSubstitutor(transferMetaData,
														metaDataMap.get("destinationFilePattern"))
												.replace("*",
														sourceFile.substring(sourceFile.lastIndexOf(linuxDelimeter) + 1,
																sourceFile.lastIndexOf("."))));
								System.out.println("in else loop " + destinationFile);
							}

						}
					} else if (metaDataMap.get("destinationFile") != null) {
						destinationFile = variablesSubstitution.variableSubstitutor(transferMetaData,
								metaDataMap.get("destinationFile"));
						System.out.println("in else if loop " + destinationFile);
					}
					if (metaDataMap.get("transferId") == null) {
						// Creating an object for UniqueID class
						UniqueID uniqueIDTest = new UniqueID();
						// Declaration of parameters transferId and
						// initialising
						// it
						// with
						// generateUniqueID
						transferId = uniqueIDTest.generateUniqueID();
					} else {
						transferId = metaDataMap.get("transferId");
					}
					System.out.println(transferId);
					// Inserting transferId, sourceFile and
					// destinationFile
					// to
					// transferMetaData
					transferMetaData.put("transferId", transferId);
					transferMetaData.put("sourceFileName", sourceFile);
					transferMetaData.put("destinationFile", destinationFile);
					System.out.println(transferMetaData);
					// Updating the database based on monitorName
					// cassandraInteracter.started(
					// cassandraInteracter.connectCassandra(),
					// metaDataMap.get("monitorName"));
					try {
						// Creating an object for KafkaSecondLayer class
						KafkaSecondLayer kafkaSecondLayer = new KafkaSecondLayer();
						// Publishing the monitor table data
						kafkaSecondLayer.publish(loadProperties.getOFTEProperties().getProperty("TOPICNAME"),
								metaDataMap.get("monitorName"), cassandraInteracter.kafkaSecondCheckMonitor(
										cassandraInteracter.connectCassandra(), metaDataMap.get("monitorName")));
					}
					// catching the exception for NoSuchFieldException
					catch (NoSuchFieldException noSuchFieldException) {
						noSuchFieldException.printStackTrace(new PrintWriter(log4jStringWriter));
						// logging the exception for
						// NoSuchFieldException
						logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION")
								+ log4jStringWriter.toString());

					}
					// catching the exception for SecurityException
					catch (SecurityException securityException) {
						securityException.printStackTrace(new PrintWriter(log4jStringWriter));
						// logging the exception for SecurityException
						logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION")
								+ log4jStringWriter.toString());
					}
					// Updating the database
					cassandraInteracter.transferDetails(cassandraInteracter.connectCassandra(), metaDataMap,
							transferMetaData);
					// Updating the database
					cassandraInteracter.transferEventDetails(cassandraInteracter.connectCassandra(), metaDataMap,
							transferMetaData);

					// metaDataMap.put("preSource", "ADD|1|2~SUB|4|1|1");
					// PreSource Condition
					if (metaDataMap.get("preSource") != null) {
						String preSource = metaDataMap.get("preSource");
						UserExitPoints userExitPoints = new UserExitPoints();
						int result = userExitPoints.accessExitPoint(preSource, metaDataMap, transferMetaData);
						System.out.println(result);
					}

					pool.execute(new WorkerThread(transferMetaData.get("sourceFileName"), zkClient, zkUtils,
							metaDataMap, transferMetaData));

					// Invoking FilesProcessorService class
					// PostDst
					System.out.println("fileProcessor releasing");
				}
			}
			pool.shutdown();
		}
		return processFileList;
	}
}
