package commm;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.naming.directory.InvalidAttributesException;
import javax.xml.transform.TransformerException;

import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

//import commm.DBOperations;
//import commm.KafkaSecondLayer;

public class XMLFileCreationss {
	static Logger logger = Logger.getLogger(XMLFileCreationss.class.getName());
	 static String loggerException="Caught exception; decorating with appropriate status template : ";
	private Map<String, String> mapUpdater(int i, String[] args, Map<String, String> metaDataMap)
			throws Exception {
//		String destinationDirectory = null, sourceDirectory = null, destiationFile = null, sourcefilePattern = null;
//		String pollInterval, pollUnits, jobName, triggerDestination, triggerPattern, xmlFilePath, monitorName,
//				destinationExists, sourceDisposition;
		try {
			switch (args[i]) {
			case "-dd":
//				destinationDirectory = args[i + 1];
//				sourceDirectory = args[i + 2];
				metaDataMap.put("destinationDirectory", args[i + 1]);
				metaDataMap.put("sourceDirectory", args[i + 2]);
				break;
			case "-df":
//				destiationFile = args[i + 1];
//				sourceDirectory = args[i + 2];
				metaDataMap.put("destiationFile", args[i + 1]);
				metaDataMap.put("sourceDirectory", args[i + 2]);
				break;
			case "-tr":
//				triggerPattern = args[i + 1];
				metaDataMap.put("triggerPattern", args[i + 1]);
				break;
			case "-trd":
//				triggerDestination = args[i + 1];
				metaDataMap.put("triggerDestination", args[i + 1]);
				break;
			case "-pi":
//				pollInterval = args[i + 1];
				metaDataMap.put("pollInterval", args[i + 1]);
				break;
			case "-pu":
//				pollUnits = args[i + 1];
				metaDataMap.put("pollUnits", args[i + 1]);
				break;
			case "-jn":
//				jobName = args[i + 1];
				metaDataMap.put("jobName", args[i + 1]);
				break;
			case "-gt":
//				xmlFilePath = args[i + 1];
				metaDataMap.put("xmlFilePath", args[i + 1]);
				break;
			case "-sfp":
//				sourcefilePattern = args[i + 1];
				metaDataMap.put("sourcefilePattern", args[i + 1]);
				break;
			case "-de":
//				destinationExists = args[i + 1];
				if ("overwrite".equalsIgnoreCase(args[i + 1]) || "error".equalsIgnoreCase(args[i + 1])) {
					metaDataMap.put("destinationExists", args[i + 1]);
				} else {
					throw new InvalidAttributesException();
				}
				break;
			case "-mn":
//				monitorName = args[i + 1];
				metaDataMap.put("monitorName", args[i + 1]);
				break;
			case "-sd":
//				sourceDisposition = args[i + 1];
				if ("delete".equalsIgnoreCase(args[i + 1]) || "leave".equalsIgnoreCase(args[i + 1])) {
					metaDataMap.put("sourceDisposition", args[i + 1]);
				} else {
					throw new InvalidAttributesException();
				}
				break;
			case "-f":
				metaDataMap.put("monitorOverride", "true");
			}

		} catch (InvalidAttributesException e1) {
			StringWriter stack = new StringWriter();
			e1.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}
		return metaDataMap;
	}

	public static void main(String[] args) throws Exception {
		try {
			String topicName="monitor";
			Map<String, String> metaDataMap = new HashMap<String, String>();
			XMLFileCreationss xmlFileCreation=new XMLFileCreationss();
			for (int i = 0; i < args.length; i++) {
				metaDataMap =xmlFileCreation.mapUpdater(i, args, metaDataMap);
			}
//			XMLCreator.access(metaDataMap);
			XMLCreator xmlCreator=new XMLCreator();
			xmlCreator.access(metaDataMap);
			
			Session session = DBOperations.connectCassandra();
			if (DBOperations.DBMonitorCheck(session, metaDataMap.get("monitorName")) == null
					|| metaDataMap.get("monitorOverride").equalsIgnoreCase("true")) {
				DBOperations.starting(session, metaDataMap.get("monitorName"));
				KafkaSecondLayer kafkaSecondLayer=new KafkaSecondLayer();
				kafkaSecondLayer.publish(topicName, metaDataMap.get("monitorName"), DBOperations.kafkaSecondCheckMonitor(session,metaDataMap.get("monitorName")));
				
			} else {
				throw new Exception("Monitor " + metaDataMap.get("monitorName")
						+ "already exists specify -f parameter to override the previous one");
			}
			session.close();
			//MyTimerTask myTimerTask=new MyTimerTask(null, null);
			MyTimerTask.timerAccess(metaDataMap);
//			MyTimerTask.timerAccess(metaDataMap);
		} catch (TransformerException transformerException) {
			StringWriter stack = new StringWriter();
			transformerException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoHostAvailableException noHostAvailableException) {
			StringWriter stack = new StringWriter();
			noHostAvailableException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoSuchFieldException noSuchFieldException) {
			StringWriter stack = new StringWriter();
			noSuchFieldException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoSuchMethodError noSuchMethodError) {
			StringWriter stack = new StringWriter();
			noSuchMethodError.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoAvailableBrokersException noAvailableBrokersException) {
			StringWriter stack = new StringWriter();
			noAvailableBrokersException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}
	}

	
}
