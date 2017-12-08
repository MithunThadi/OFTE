package com.ofte.services;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import javax.naming.directory.InvalidAttributesException;

import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

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

public class MetaDataCreations {
	// Creating an object for LoadProperties class
	static LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for MetaDataCreations class
	static Logger logger = Logger.getLogger(MetaDataCreations.class.getName());
	// Creating an object for StringWriter class
	static StringWriter log4jStringWriter = new StringWriter();
	// Declaration of parameter Timer
	static Timer timer;
	/**
	 * This method is used to put the command values into the map object
	 * 
	 * @param i
	 * @param str
	 * @param metaDataMap
	 * @return metaDataMap
	 */
	@SuppressWarnings("rawtypes")
	public HashMap<String, String> mapUpdater(int i,
			HashMap<String, String> map, HashMap<String, String> metaDataMap) {
		try {

			for (Map.Entry metaData : map.entrySet()) {
				if (metaData.getKey().equals("-mn")) {
					metaDataMap.put("monitorName",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-sn")) {
					metaDataMap.put("schedulerName",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-jn")) {
					metaDataMap.put("jobName", metaData.getValue().toString());
				} else if (metaData.getKey().equals("sourceDirectory")) {
					metaDataMap.put("sourceDirectory",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-sftp-s")) {
					metaDataMap.put("sftpAsSource",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-sftp-d")) {
					metaDataMap.put("sftpAsDestination",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-sfp")) {
					metaDataMap.put("sourcefilePattern",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-tr")) {
					metaDataMap.put("triggerPattern",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-dd")) {
					metaDataMap.put("destinationDirectory",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-trd")) {
					metaDataMap.put("destinationTriggerPattern",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-dfp")) {
					metaDataMap.put("destinationFilePattern",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-pu")) {
					metaDataMap.put("pollUnits",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-pi")) {
					metaDataMap.put("pollInterval",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-gt")) {
					metaDataMap.put("xmlFilePath",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-hi")) {
					metaDataMap.put("hostIp", metaData.getValue().toString());
				} else if (metaData.getKey().equals("-un")) {
					metaDataMap.put("userName", metaData.getValue().toString());
				} else if (metaData.getKey().equals("-pw")) {
					metaDataMap.put("password", metaData.getValue().toString());
				} else if (metaData.getKey().equals("-po")) {
					metaDataMap.put("port", metaData.getValue().toString());
				} else if (metaData.getKey().equals("-sd")) {
					metaDataMap.put("sourceDisposition",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-de")) {
					metaDataMap.put("destinationExists",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-f")) {
					metaDataMap.put("monitorOverWrite",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-preSrc")) {
					metaDataMap.put("preSource",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-preDst")) {
					metaDataMap.put("preDestination",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-postSrc")) {
					metaDataMap.put("postSource",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-postDst")) {
					metaDataMap.put("postDestination",
							metaData.getValue().toString());
				}

			}
		}
		// catching the exception for InvalidAttributesException
		catch (Exception e1) {
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
	public void fetchingUIDetails(HashMap<String, String> hashMap)
			throws Exception {
		try {
			// Creating an object for Map
			HashMap<String, String> metaDataMap = new HashMap<String, String>();
			// Creating an object for MetaDataCreations class
			// MetaDataCreations metaDataCreations=new MetaDataCreations();
			// Creating an object for CassandraInteracter class
			CassandraInteracter cassandraInteracter = new CassandraInteracter();

			KafkaMapData kafkaMapData = new KafkaMapData();
			// System.out.println(hashMap +" in fetching");
			// for loop to increment i value until i less than args.length
			for (int i = 0; i < hashMap.size(); i++) {
				// System.out.println("Hello");
				metaDataMap = mapUpdater(i, hashMap, metaDataMap);
			}
			System.out.println(metaDataMap + " after mapupdater");

			// preSource Condition
			// if(metaDataMap.get("preSource") != null) {
			// String className = ((metaDataMap.get("preSource")).substring(0,
			// (metaDataMap.get("preSource")).indexOf("|")));
			// }

			// Creating an object for XMLCreator class
			XMLCreator xmlCreator = new XMLCreator();

			if (metaDataMap.get("xmlFilePath") == null
					&& metaDataMap.get("monitorName") == null) {
				if (metaDataMap.get("schedulerName") == null) {
					if (metaDataMap.get("sourceDirectory") != null
							&& metaDataMap.get("triggerPattern") != null) {

						OneTimeTransfer oneTimeTransfer = new OneTimeTransfer();
						oneTimeTransfer.transfer(metaDataMap);
					} else {
						System.out.println("in one time transfer");
						throw new InvalidAttributesException();
					}
				} else {
					if (metaDataMap.get("schedulerName") != null
							&& metaDataMap.get("jobName") != null
							&& metaDataMap.get("hostIp") != null
							&& metaDataMap.get("userName") != null
							&& metaDataMap.get("password") != null
							&& metaDataMap.get("port") != null) {
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
								metaDataMap.toString().substring(1,
										metaDataMap.toString().length() - 1)
										.replace(" ", ""));
						cassandraInteracter.insertScheduleMetaData(
								cassandraInteracter.connectCassandra(),
								metaDataMap.get("schedulerName"),
								metaDataMap.toString().substring(1,
										metaDataMap.toString().length() - 1)
										.replace(" ", ""));

						SFTPTimedMonitor sftpTimedMonitor = new SFTPTimedMonitor();
						sftpTimedMonitor.timerAccess(
								metaDataMap.get("schedulerName"),
								metaDataMap.get("pollInterval"),
								metaDataMap.get("pollUnits"));
						System.out.println("sftp done successfully");
					} else {
						System.out.println("in sftp");
						throw new InvalidAttributesException();
					}

				}

				if (cassandraInteracter.DBMonitorCheck(
						cassandraInteracter.connectCassandra(),
						metaDataMap.get("monitorName")) == null) {
					cassandraInteracter.starting(
							cassandraInteracter.connectCassandra(),
							metaDataMap.get("monitorName"));
				}

			} else if (metaDataMap.get("xmlFilePath") != null
					&& metaDataMap.get("monitorName") != null) {
				if (metaDataMap.get("jobName") != null
						&& metaDataMap.get("sourceDirectory") != null
						&& metaDataMap.get("triggerPattern") != null
						&& metaDataMap.get("pollInterval") != null
						&& metaDataMap.get("pollUnits") != null) {
					// Creating the xml file as per given command

					xmlCreator.access(metaDataMap);
					if (cassandraInteracter.DBMonitorCheck(
							cassandraInteracter.connectCassandra(),
							metaDataMap.get("monitorName")) == null
							|| metaDataMap.get("monitorOverWrite")
									.equalsIgnoreCase("Yes")) {
						cassandraInteracter.starting(
								cassandraInteracter.connectCassandra(),
								metaDataMap.get("monitorName"));

						// declaration of parameter mapTopicName and
						// initialising
						// the
						// mapTopicName with monitor name
						String mapTopicName = "Monitor_MetaData_"
								+ metaDataMap.get("monitorName");

						// Publishing map data
						kafkaMapData.publish(mapTopicName,
								metaDataMap.get("monitorName"),
								metaDataMap.toString().substring(1,
										metaDataMap.toString().length() - 1)
										.replace(" ", ""));
						// have to add code to insert data in cassandra
						cassandraInteracter.insertMonitorMetaData(
								cassandraInteracter.connectCassandra(),
								metaDataMap.get("monitorName"),
								metaDataMap.toString().substring(1,
										metaDataMap.toString().length() - 1)
										.replace(" ", ""));

						// Creating an object for TimedMonitor class
						TimedMonitor timedMonitor = new TimedMonitor();
						// initialising the poll time by taking pollInterval and
						// pollunits
						timedMonitor.timerAccess(metaDataMap.get("monitorName"),
								metaDataMap.get("pollInterval"),
								metaDataMap.get("pollUnits"));
					} else {
						throw new Exception("Monitor "
								+ metaDataMap.get("monitorName")
								+ "already exists click for MonitorOverwrite to override the previous one");
					}

				}

				else {
					throw new InvalidAttributesException();
				}

			}
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
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