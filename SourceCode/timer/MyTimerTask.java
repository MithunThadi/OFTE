package commm;

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

//import cos.DBOperations;
//import cos.KafkaSecondLayer;
import io.netty.handler.timeout.TimeoutException;

public class MyTimerTask extends TimerTask {
	static Logger logger = Logger.getLogger(MyTimerTask.class.getName());
	static String loggerException="Caught exception; decorating with appropriate status template : " ;
	static Map<String, String> metaDataMap1;
	static SimpleDateFormat simpledateFormat = new SimpleDateFormat("ddHHmmss");
//	int counter ;
	int previousListSize = 0;
	File file;
	String[] filesInDirectory;
	LinkedList<String> filesList = new LinkedList<String>();
	LinkedList<String> matchedFilesList = new LinkedList<String>();
	LinkedList<String> processFileList = new LinkedList<String>();
	Timer timer;
	long pollTime;
	Map<String, String> transferMetaData = new HashMap<String, String>();
	String topicName="monitor";

	public MyTimerTask(Timer timer,Long pollTime) {
		this.timer = timer;
		this.pollTime = pollTime;
	}

	public static void timerAccess(Map<String, String> metaDataMap) {
		try {
			metaDataMap1 = metaDataMap;
			Timer timer = new Timer();
			int pollInterval = Integer.parseInt(metaDataMap.get("pollInterval"));
			String pollUnits = metaDataMap.get("pollUnits");
			long pollTime = 0;
			switch (pollUnits) {
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

			timer.scheduleAtFixedRate(new MyTimerTask(timer, pollTime), 1000, pollTime);
		} catch (NumberFormatException numberFormatException) {
			StringWriter stack = new StringWriter();
			numberFormatException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}
	}

	@Override
	public void run() {
		try {
//			processFileList.clear();
			file = new File(metaDataMap1.get("sourceDirectory"));
			System.out.println("Timer created for::" + file);
			int numberOfFiles = file.listFiles().length;
			System.out.println(numberOfFiles);
			filesInDirectory = file.list();
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
			System.out.println("Hello");
			System.out.println(filesInDirectory.length);
			for (int i = 0; i < filesInDirectory.length; i++) {
				System.out.println((!filesInDirectory[i].equalsIgnoreCase(""))+ " "+  (TriggerPatternValidator
						.validateTriggerPattern(metaDataMap1.get("triggerPattern"), filesInDirectory[i]) + " " + filesInDirectory[i]));
				if ((!filesInDirectory[i].equalsIgnoreCase("")) && (TriggerPatternValidator
						.validateTriggerPattern(metaDataMap1.get("triggerPattern"), filesInDirectory[i]))) {
					filesList.add(filesInDirectory[i]);
					System.out.println("tpv");
				}
			}
			System.out.println(filesList.size());
			System.out.println("Hi");
			if (previousListSize < filesList.size()) {
				System.out.println(filesList);
				int count = (filesList.size() - (filesList.size() - previousListSize));
				for (int i = count; i < filesList.size(); i++) {
					System.out.println(filesList.get(i));
					if (TriggerPatternValidator.validateTriggerPattern(metaDataMap1.get("triggerPattern"),
							filesList.get(i).toString())) {
						processFileList.add((filesList.get(i)).toString());
						System.out.println("tpv");
					}
				}
			}
			FilesProcessor fileProcessor = new FilesProcessor();
			
			if (processFileList.size() > 0) {
//				ExecutorService pool = Executors.newFixedThreadPool(processFileList.size());
				for (String file : processFileList) {
					String sourceFile=null,destinationFile=null;
					System.out.println(file);
					String filePath = metaDataMap1.get("sourceDirectory") + "\\" + file;
					transferMetaData.put("FileName", file);
					transferMetaData.put("FilePath", filePath);
					if(metaDataMap1.get("triggerPattern").equalsIgnoreCase(metaDataMap1.get("sourcefilePattern"))) {
						sourceFile=filePath;
					}else if(metaDataMap1.get("sourcefilePattern")!=null) {
						sourceFile=VariablesSubstitution.variableSubstitutor(transferMetaData, metaDataMap1.get("sourcefilePattern"));
					}
					String targetFile = metaDataMap1.get("destinationDirectory") + sourceFile.substring(sourceFile.lastIndexOf("\\"));
					if(metaDataMap1.get("destinationDirectory")!=null) {
						destinationFile=targetFile;
					}else if(metaDataMap1.get("destinationFile")!=null) {
						destinationFile= VariablesSubstitution.variableSubstitutor(transferMetaData, metaDataMap1.get("destinationFile"));
					}
					//Session session = DBOperations.connectCassandra();
					String transferId = UniqueIDTest.generateUniqueID();
					System.out.println(transferId);
					transferMetaData.put("transferId", transferId);
					transferMetaData.put("sourceFileName", sourceFile);
					transferMetaData.put("destinationFile", destinationFile);
					DBOperations.started(DBOperations.connectCassandra(), metaDataMap1.get("monitorName"));
					try {
						KafkaSecondLayer kafkaSecondLayer=new KafkaSecondLayer();
						kafkaSecondLayer.publish(topicName, metaDataMap1.get("monitorName"), DBOperations.kafkaSecondCheckMonitor(DBOperations.connectCassandra(),metaDataMap1.get("monitorName")));
					} catch (NoSuchFieldException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
					DBOperations.transferDetails(DBOperations.connectCassandra(), metaDataMap1, transferMetaData);
					
					DBOperations.transferEventDetails(DBOperations.connectCassandra(), metaDataMap1, transferMetaData);
//					session.close();
//					pool.execute(new WorkerThread(metaDataMap1, transferMetaData));
					//Lock lock = new ReentrantLock();
					
					//lock.lock();
					fileProcessor.getMessages(filePath, metaDataMap1, transferMetaData);
					System.out.println("fileProcessor releasing");
					//lock.unlock();
				}
			}
			processFileList.clear();
			

		} catch (NoSuchMethodError noSuchMethodError) {
			StringWriter stack = new StringWriter();
			noSuchMethodError.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (NoHostAvailableException noHostAvailableException) {
			StringWriter stack = new StringWriter();
			noHostAvailableException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException+ stack.toString());
		} catch (TimeoutException timeoutException) {
			StringWriter stack = new StringWriter();
			timeoutException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (org.apache.kafka.common.errors.TimeoutException apachecommonTimeoutException) {
			StringWriter stack = new StringWriter();
			apachecommonTimeoutException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (org.jboss.netty.handler.timeout.TimeoutException jbossTimeoutException) {
			StringWriter stack = new StringWriter();
			jbossTimeoutException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}
	}

}
