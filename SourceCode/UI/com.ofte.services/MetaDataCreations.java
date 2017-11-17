package com.ofte.services;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

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
			// switch case
			// System.out.println(str[i]);
			// System.out.println(map +" in map");
			for (Map.Entry metaData : map.entrySet()) {
				// System.out.println(metaData +" in for loop");
				// switch (metaData.getKey().toString()) {
				// //Depending upon the case we are setting the values into
				// metaDataMap through the keys
				// case "-sd":
				//// metaDataMap.put("destinationDirectory", str[i + 1]);
				// metaDataMap.put("sourceDirectory",
				// metaData.getValue().toString());
				// break;
				// case "-dd":
				// metaDataMap.put("destinationDirectory",
				// metaData.getValue().toString());
				//// metaDataMap.put("sourceDirectory", str[i + 2]);
				// break;
				//// case "-df":
				//// metaDataMap.put("destiationFile", str[i + 1]);
				//// metaDataMap.put("sourceDirectory", str[i + 2]);
				//// break;
				// case "-tr":
				// metaDataMap.put("triggerPattern",
				// metaData.getValue().toString());
				// break;
				// case "-trd":
				// metaDataMap.put("triggerDestination",
				// metaData.getValue().toString());
				// break;
				// case "-pi":
				// metaDataMap.put("pollInterval",
				// metaData.getValue().toString());
				// break;
				// case "-pu":
				// metaDataMap.put("pollUnits", metaData.getValue().toString());
				// break;
				// case "-jn":
				// metaDataMap.put("jobName", metaData.getValue().toString());
				// break;
				// case "-gt":
				// metaDataMap.put("xmlFilePath",
				// metaData.getValue().toString());
				// break;
				// case "-sfp":
				// metaDataMap.put("sourcefilePattern",
				// metaData.getValue().toString());
				// break;
				// case "-rs" :
				// metaDataMap.put("sftpAsSource",
				// metaData.getValue().toString());
				// case "-rd" :
				// metaDataMap.put("sftpAsDestination",
				// metaData.getValue().toString());
				//
				// case "-sn" :
				// metaDataMap.put("schedulerName",
				// metaData.getValue().toString());
				// case "-de":
				// if
				// ("overwrite".equalsIgnoreCase(metaData.getValue().toString())
				// || "error".equalsIgnoreCase(metaData.getValue().toString()))
				// {
				// metaDataMap.put("destinationExists",
				// metaData.getValue().toString());
				// } else {
				// throw new InvalidAttributesException();
				// }
				// break;
				// case "-mn":
				// metaDataMap.put("monitorName",metaData.getValue().toString());
				// break;
				// case "-sfd":
				// if ("delete".equalsIgnoreCase(metaData.getValue().toString())
				// || "leave".equalsIgnoreCase(metaData.getValue().toString()))
				// {
				// metaDataMap.put("sourceDisposition",metaData.getValue().toString());
				// } else {
				// throw new InvalidAttributesException();
				// }
				// break;
				// case "-f":
				// metaDataMap.put("monitorOverride", "true");
				// }
				if (metaData.getKey().equals("-mn")) {
					metaDataMap.put("monitorName",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-sn")) {
					metaDataMap.put("schedulerName",
							metaData.getValue().toString());
				} else if (metaData.getKey().equals("-jn")) {
					metaDataMap.put("jobName", metaData.getValue().toString());
				} else if (metaData.getKey().equals("-sd")) {
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
					metaDataMap.put("triggerDestination",
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
			// System.out.println(metaDataMap.get("monitorName"));
			// System.out.println(metaDataMap.get("xmlFilePath"));
			// System.out.println(metaDataMap.get("monitorName"));
			// System.out.println(metaDataMap.get("schedulerName"));
			// Creating an object for XMLCreator class
			XMLCreator xmlCreator = new XMLCreator();

			if (metaDataMap.get("xmlFilePath") == null
					&& metaDataMap.get("schedulerName") == null) {
				if (cassandraInteracter.DBMonitorCheck(
						cassandraInteracter.connectCassandra(),
						metaDataMap.get("monitorName")) == null) {
					cassandraInteracter.starting(
							cassandraInteracter.connectCassandra(),
							metaDataMap.get("monitorName"));
				}
				OneTimeTransfer oneTimeTransfer = new OneTimeTransfer();
				oneTimeTransfer.transfer(metaDataMap);
			} else if (metaDataMap.get("xmlFilePath") != null
					&& metaDataMap.get("monitorName") != null) {
				// Creating the xml file as per given command
				try {
					xmlCreator.access(metaDataMap);
					if (cassandraInteracter.DBMonitorCheck(
							cassandraInteracter.connectCassandra(),
							metaDataMap.get("monitorName")) == null) {
						cassandraInteracter.starting(
								cassandraInteracter.connectCassandra(),
								metaDataMap.get("monitorName"));
					}

				} catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				// declaration of parameter mapTopicName and initialising
				// the
				// mapTopicName with monitor name
				String mapTopicName = "Monitor_MetaData_"
						+ metaDataMap.get("monitorName");

				// if(cassandraInteracter.DBMonitorCheck(cassandraInteracter.connectCassandra(),
				// metaDataMap.get("monitorName")) == null ||
				// metaDataMap.get("monitorOverride").equalsIgnoreCase("true"))
				// {
				// cassandraInteracter.starting(cassandraInteracter.connectCassandra(),
				// metaDataMap.get("monitorName"));
				// }

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
					sftpTimedMonitor.timerAccess(
							metaDataMap.get("schedulerName"),
							metaDataMap.get("pollInterval"),
							metaDataMap.get("pollUnits"));
					System.out.println("sftp done successfully");

				} else {
					System.out.println("please remove monitor name");
				}

			}
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