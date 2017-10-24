package com.ofte.services;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import javax.naming.directory.InvalidAttributesException;
import javax.xml.transform.TransformerException;

import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
/**
 * 
 * Class Functionality:
 * 						The main functionality of this class is creation of XMLFile and publishing the files in the source directory based on the user command and parallelly updating the DB
 * 
 * Methods:
 * 			public Map<String, String> mapUpdater(int i, String[] args, Map<String, String> metaDataMap)
 * 			static void main(String[] args) throws Exception
 *
 */
public class MetaDataCreations {
	//Creating an object for LoadProperties class
	static LoadProperties loadProperties = new LoadProperties();
	//Creating Logger object for MetaDataCreations class
	 static Logger logger = Logger.getLogger(MetaDataCreations.class.getName());
	//Creating an object for StringWriter class
	 static StringWriter log4jStringWriter = new StringWriter();
	 //Declaration of parameter Timer
	 static Timer timer ;
	 /**
	   * 
	   * @param i
	   * @param args
	   * @param metaDataMap
	   * @return metaDataMap
	   */
	public Map<String, String> mapUpdater(int i, String[] args, Map<String, String> metaDataMap)			
	{	
		try {
			//switch case
			switch (args[i]) {
			//Depending upon the case we are setting the values into metaDataMap through the keys
			case "-dd":
				metaDataMap.put("destinationDirectory", args[i + 1]);
				metaDataMap.put("sourceDirectory", args[i + 2]);
				break;
			case "-df":
				metaDataMap.put("destiationFile", args[i + 1]);
				metaDataMap.put("sourceDirectory", args[i + 2]);
				break;
			case "-tr":
				metaDataMap.put("triggerPattern", args[i + 1]);
				break;
			case "-trd":
				metaDataMap.put("triggerDestination", args[i + 1]);
				break;
			case "-pi":
				metaDataMap.put("pollInterval", args[i + 1]);
				break;
			case "-pu":
				metaDataMap.put("pollUnits", args[i + 1]);
				break;
			case "-jn":
				metaDataMap.put("jobName", args[i + 1]);
				break;
			case "-gt":
				metaDataMap.put("xmlFilePath", args[i + 1]);
				break;
			case "-sfp":
				metaDataMap.put("sourcefilePattern", args[i + 1]);
				break;
			case "-de":
				if ("overwrite".equalsIgnoreCase(args[i + 1]) || "error".equalsIgnoreCase(args[i + 1])) {
					metaDataMap.put("destinationExists", args[i + 1]);
				} else {
					throw new InvalidAttributesException();
				}
				break;
			case "-mn":
				metaDataMap.put("monitorName", args[i + 1]);
				break;
			case "-sd":
				if ("delete".equalsIgnoreCase(args[i + 1]) || "leave".equalsIgnoreCase(args[i + 1])) {
					metaDataMap.put("sourceDisposition", args[i + 1]);
				} else {
					throw new InvalidAttributesException();
				}
				break;
			case "-f":
				metaDataMap.put("monitorOverride", "true");
			}

		} 
		//catching the exception for InvalidAttributesException
		catch (InvalidAttributesException e1) {
			e1.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for InvalidAttributesException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//return statement
		return metaDataMap;
	}
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			//Creating an object for Map
			Map<String, String> metaDataMap = new HashMap<String, String>();
			//Creating an object for MetaDataCreations class
			MetaDataCreations metaDataCreations=new MetaDataCreations();
			//Creating an object for CassandraInteracter class
			CassandraInteracter cassandraInteracter=new CassandraInteracter();
			//for loop to increment i value until i less than args.length
			for (int i = 0; i < args.length; i++) {
				metaDataMap =metaDataCreations.mapUpdater(i, args, metaDataMap);
			}
			//Creating an object for XMLCreator class
			XMLCreator xmlCreator=new XMLCreator();
			xmlCreator.access(metaDataMap);
			//Creation of Session object
			Session session = cassandraInteracter.connectCassandra();
			//if loop to check the conditions from database
			if (cassandraInteracter.DBMonitorCheck(session, metaDataMap.get("monitorName")) == null
					|| metaDataMap.get("monitorOverride").equalsIgnoreCase("true")) {
				cassandraInteracter.starting(session, metaDataMap.get("monitorName"));
				//Creating an object for KafkaSecondLayer class
				KafkaSecondLayer kafkaSecondLayer=new KafkaSecondLayer();
				kafkaSecondLayer.publish(loadProperties.getOFTEProperties().getProperty("TOPICNAME"), metaDataMap.get("monitorName"), cassandraInteracter.kafkaSecondCheckMonitor(session,metaDataMap.get("monitorName")));
				
			} else {
				throw new Exception("Monitor " + metaDataMap.get("monitorName")
						+ "already exists specify -f parameter to override the previous one");
			}
			//closing session object
			session.close();
			//Creating an object for TimedMonitor class
			TimedMonitor timedMonitor=new TimedMonitor(timer, 0L);
			timedMonitor.timerAccess(metaDataMap);
			} 
		//catching the exception for TransformerException
		catch (TransformerException transformerException) {
			transformerException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for TransformerException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		} 
		//catching the exception for NoHostAvailableException
		catch (NoHostAvailableException noHostAvailableException) {
			noHostAvailableException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoHostAvailableException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for NoSuchFieldException
		catch (NoSuchFieldException noSuchFieldException) {
			noSuchFieldException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoSuchFieldException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for NoSuchMethodError
		catch (NoSuchMethodError noSuchMethodError) {
			noSuchMethodError.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoSuchMethodError
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for NoAvailableBrokersException
		catch (NoAvailableBrokersException noAvailableBrokersException) {
			noAvailableBrokersException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for NoAvailableBrokersException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for InvalidQueryException
		catch (InvalidQueryException invalidQueryException) {
			invalidQueryException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for InvalidQueryException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}	
}