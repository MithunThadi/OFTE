package ofte;

import java.util.HashMap;
import java.util.Map;

import javax.naming.directory.InvalidAttributesException;

import com.datastax.driver.core.Session;

public class XMLFileCreation {

	public static void main(String[] args) throws Exception {
		Map<String, String> metaDataMap = new HashMap<String, String>();
		for (int i = 0; i < args.length; i++) {
			metaDataMap = mapUpdater(i, args, metaDataMap);

		}
		XMLCreator.access(metaDataMap);
		Session session = DBOperations.connectCassandra();
		if (DBOperations.DBMonitorCheck(session, metaDataMap.get("monitorName")) == null
				|| metaDataMap.get("monitorOverride").equalsIgnoreCase("true")) {
			DBOperations.starting(session, metaDataMap.get("monitorName"));
		} else {
			throw new Exception("Monitor " + metaDataMap.get("monitorName")
					+ "already exists specify -f parameter to override the previous one");
		}
		session.close();
		MyTimerTask.timerAccess(metaDataMap);
	}

	private static Map<String, String> mapUpdater(int i, String[] args, Map<String, String> metaDataMap)
			throws InvalidAttributesException {
		String destinationDirectory = null, sourceDirectory = null, destiationFile = null, sourcefilePattern = null;
		String pollInterval, pollUnits, jobName, triggerDestination, triggerPattern, xmlFilePath, monitorName,
				destinationExists, sourceDisposition;

		switch (args[i]) {
		case "-dd":
			destinationDirectory = args[i + 1];
			sourceDirectory = args[i + 2];
			metaDataMap.put("destinationDirectory", destinationDirectory);
			metaDataMap.put("sourceDirectory", sourceDirectory);
			break;
		case "-df":
			destiationFile = args[i + 1];
			sourceDirectory = args[i + 2];
			metaDataMap.put("destiationFile", destiationFile);
			metaDataMap.put("sourceDirectory", sourceDirectory);
			break;
		case "-tr":
			triggerPattern = args[i + 1];
			metaDataMap.put("triggerPattern", triggerPattern);
			break;
		case "-trd":
			triggerDestination = args[i + 1];
			metaDataMap.put("triggerDestination", triggerDestination);
			break;
		case "-pi":
			pollInterval = args[i + 1];
			metaDataMap.put("pollInterval", pollInterval);
			break;
		case "-pu":
			pollUnits = args[i + 1];
			metaDataMap.put("pollUnits", pollUnits);
			break;
		case "-jn":
			jobName = args[i + 1];
			metaDataMap.put("jobName", jobName);
			break;
		case "-gt":
			xmlFilePath = args[i + 1];
			metaDataMap.put("xmlFilePath", xmlFilePath);
			break;
		case "-sfp":
			sourcefilePattern = args[i + 1];
			metaDataMap.put("sourcefilePattern", sourcefilePattern);
			break;
		case "-de":
			destinationExists = args[i + 1];
			if ("overwrite".equalsIgnoreCase(destinationExists) || "error".equalsIgnoreCase(destinationExists)) {
				metaDataMap.put("destinationExists", destinationExists);
			} else {
				throw new InvalidAttributesException();
			}
			break;
		case "-mn":
			monitorName = args[i + 1];
			metaDataMap.put("monitorName", monitorName);
			break;
		case "-sd":
			sourceDisposition = args[i + 1];
			if ("delete".equalsIgnoreCase(sourceDisposition) || "leave".equalsIgnoreCase(sourceDisposition)) {
				metaDataMap.put("sourceDisposition", sourceDisposition);
			} else {
				throw new InvalidAttributesException();
			}
			break;
		case "-f":
			metaDataMap.put("monitorOverride", "true");
		}

		return metaDataMap;
	}
}
