package com.sftp.services;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import javax.naming.directory.InvalidAttributesException;

import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
/**
 * 
 * Class Functionality: The main functionality of this class is creation of
 * XMLFile and publishing the files in the source directory based on the user
 * command and parallelly updating the DB
 * 
 * Methods: public Map<String, String> mapUpdater(int i, String[] args,
 * Map<String, String> metaDataMap) static void main(String[] args) throws
 * Exception
 *
 */

public class MetaDataCreations2 {
	// Creating an object for LoadProperties class
	static LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for MetaDataCreations class
	static Logger logger = Logger.getLogger(MetaDataCreations2.class.getName());
	// Creating an object for StringWriter class
	static StringWriter log4jStringWriter = new StringWriter();
	// Declaration of parameter Timer
	static Timer timer;
	/**
	 * This method is used to put the command values into the map object
	 * 
	 * @param i
	 * @param args
	 * @param metaDataMap
	 * @return metaDataMap
	 */
	public Map<String, String> mapUpdater(int i, String[] args,
			Map<String, String> metaDataMap) {
		try {
			// switch case
			switch (args[i]) {
				// Depending upon the case we are setting the values into
				// metaDataMap through the keys
				case "-dd" :
					metaDataMap.put("destinationDirectory", args[i + 1]);
					metaDataMap.put("sourceDirectory", args[i + 2]);
					break;
				case "-df" :
					metaDataMap.put("destiationFile", args[i + 1]);
					metaDataMap.put("sourceDirectory", args[i + 2]);
					break;
				case "-tr" :
					metaDataMap.put("triggerPattern", args[i + 1]);
					break;
				case "-trd" :
					metaDataMap.put("triggerDestination", args[i + 1]);
					break;
				case "-pi" :
					metaDataMap.put("pollInterval", args[i + 1]);
					break;
				case "-pu" :
					metaDataMap.put("pollUnits", args[i + 1]);
					break;
				case "-jn" :
					metaDataMap.put("jobName", args[i + 1]);
					break;
				case "-gt" :
					metaDataMap.put("xmlFilePath", args[i + 1]);
					break;
				case "-sfp" :
					metaDataMap.put("sourcefilePattern", args[i + 1]);
					break;
				case "-sftp-s" :
					metaDataMap.put("sftpAsSource", args[i + 1]);
				case "-sftp-d" :
					metaDataMap.put("sftpAsDestination", args[i + 1]);

				case "-sn" :
					metaDataMap.put("schedulerName", args[i + 1]);

				case "-de" :
					if ("overwrite".equalsIgnoreCase(args[i + 1])
							|| "error".equalsIgnoreCase(args[i + 1])) {
						metaDataMap.put("destinationExists", args[i + 1]);
					} else {
						throw new InvalidAttributesException();
					}
					break;
				case "-mn" :
					metaDataMap.put("monitorName", args[i + 1]);
					break;
				case "-sd" :
					if ("delete".equalsIgnoreCase(args[i + 1])
							|| "leave".equalsIgnoreCase(args[i + 1])) {
						metaDataMap.put("sourceDisposition", args[i + 1]);
					} else {
						throw new InvalidAttributesException();
					}
					break;
				case "-f" :
					metaDataMap.put("monitorOverride", "true");
			}

		}
		// catching the exception for InvalidAttributesException
		catch (InvalidAttributesException e1) {
			e1.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for InvalidAttributesException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// return statement
		return metaDataMap;
	}
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			// Creating an object for Map
			Map<String, String> metaDataMap = new HashMap<String, String>();
			// Creating an object for MetaDataCreations class
			MetaDataCreations2 metaDataCreations = new MetaDataCreations2();
			// Creating an object for CassandraInteracter class
			CassandraInteracter cassandraInteracter = new CassandraInteracter();

			KafkaMapData kafkaMapData = new KafkaMapData();
			// for loop to increment i value until i less than args.length
			for (int i = 0; i < args.length; i++) {
				metaDataMap = metaDataCreations.mapUpdater(i, args,
						metaDataMap);
			}

			// Creation of Session object
			// Session session = cassandraInteracter.connectCassandra();
			// if loop to check the conditions from database
			// if (cassandraInteracter.DBMonitorCheck(
			// cassandraInteracter.connectCassandra(),
			// metaDataMap.get("monitorName")) == null
			// || metaDataMap.get("monitorOverride")
			// .equalsIgnoreCase("true")) {
			// cassandraInteracter.starting(
			// cassandraInteracter.connectCassandra(),
			// metaDataMap.get("monitorName"));
			// // Creating an object for KafkaSecondLayer class
			// KafkaSecondLayer kafkaSecondLayer = new KafkaSecondLayer();
			// // publishing the monitor Table details to the kafka server
			// kafkaSecondLayer.publish(
			// loadProperties.getOFTEProperties()
			// .getProperty("TOPICNAME"),
			// metaDataMap.get("monitorName"),
			// cassandraInteracter.kafkaSecondCheckMonitor(
			// cassandraInteracter.connectCassandra(),
			// metaDataMap.get("monitorName")));
			//
			// } else {
			// throw new Exception("Monitor " + metaDataMap.get("monitorName")
			// + "already exists specify -f parameter to override the previous
			// one");
			// }

			// Creating an object for XMLCreator class
			XMLCreator xmlCreator = new XMLCreator();
			// if loop to check the condition monitorName not equals to empty
			if (metaDataMap.get("xmlFilePath") == null
					&& metaDataMap.get("monitorName") == null) {

				OneTimeTransfer oneTimeTransfer = new OneTimeTransfer();
				oneTimeTransfer.transfer(metaDataMap);
			} else if (metaDataMap.get("xmlFilePath") != null
					&& metaDataMap.get("monitorName") != null) {
				// Creating the xml file as per given command
				xmlCreator.access(metaDataMap);

				// declaration of parameter mapTopicName and initialising the
				// mapTopicName with monitor name
				String mapTopicName = "Monitor_MetaData_"
						+ metaDataMap.get("monitorName");

				// Publishing map data
				kafkaMapData.publish(mapTopicName,
						metaDataMap.get("monitorName"),
						metaDataMap.toString().replace("{", "").replace("}", "")
								.replace(" ", ""));
				// have to add code to insert data in cassandra
				cassandraInteracter.insertMonitorMetaData(
						cassandraInteracter.connectCassandra(),
						metaDataMap.get("monitorName"),
						metaDataMap.toString().replace("{", "").replace("}", "")
								.replace(" ", ""));

				// Creating an object for TimedMonitor class
				TimedMonitor timedMonitor = new TimedMonitor();
				// initialising the poll time by taking pollInterval and
				// pollunits
				timedMonitor.timerAccess(metaDataMap.get("monitorName"),
						metaDataMap.get("pollInterval"),
						metaDataMap.get("pollUnits"));
			} else if (metaDataMap.get("sftpAsSource") != null
					|| metaDataMap.get("sftpAsDestination") != null) {
				if (metaDataMap.get("monitorName") == null) {

					cassandraInteracter.schedulerStarting(
							cassandraInteracter.connectCassandra(),
							metaDataMap.get("schedulerName"));

					// SFTPOperations sftpOperations=new SFTPOperations();
					// sftpOperations.downloadFile(metaDataMap.get("sftpAsSource"));
					String schedulerTopicName = "Scheduler_MetaData_"
							+ metaDataMap.get("schedulerName");

					// Publishing map data
					kafkaMapData.publish(schedulerTopicName,
							metaDataMap.get("schedulerName"),
							metaDataMap.toString().replace("{", "")
									.replace("}", "").replace(" ", ""));
					cassandraInteracter.insertScheduleMetaData(
							cassandraInteracter.connectCassandra(),
							metaDataMap.get("schedulerName"),
							metaDataMap.toString().replace("{", "")
									.replace("}", "").replace(" ", ""));

					SFTPTimedMonitor sftpTimedMonitor = new SFTPTimedMonitor();
					sftpTimedMonitor.timerAccess(metaDataMap,
							metaDataMap.get("pollInterval"),
							metaDataMap.get("pollUnits"));
					System.out.println("sftp done successfully");

				} else {
					System.out.println("please remove monitor name");
				}

			}
		}
		// catching the exception for TransformerException
		// catch (TransformerException transformerException) {
		// transformerException
		// .printStackTrace(new PrintWriter(log4jStringWriter));
		// // logging the exception for TransformerException
		// logger.error(loadProperties.getOFTEProperties().getProperty(
		// "LOGGEREXCEPTION") + log4jStringWriter.toString());
		// }
		// catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for NoSuchFieldException
		// catch (NoSuchFieldException noSuchFieldException) {
		// noSuchFieldException
		// .printStackTrace(new PrintWriter(log4jStringWriter));
		// // logging the exception for NoSuchFieldException
		// logger.error(loadProperties.getOFTEProperties().getProperty(
		// "LOGGEREXCEPTION") + log4jStringWriter.toString());
		// }
		// catching the exception for NoSuchMethodError
		catch (NoSuchMethodError noSuchMethodError) {
			noSuchMethodError
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoSuchMethodError
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for NoAvailableBrokersException
		catch (NoAvailableBrokersException noAvailableBrokersException) {
			noAvailableBrokersException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoAvailableBrokersException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for InvalidQueryException
		catch (InvalidQueryException invalidQueryException) {
			invalidQueryException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for InvalidQueryException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
}