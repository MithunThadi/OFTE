package ofte;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Session;

public class MyTimerTask extends TimerTask {
	static Map<String, String> metaDataMap1;
	static SimpleDateFormat simpledateFormat = new SimpleDateFormat("ddHHmmss");
	int counter = 0, previousList = 0;
	File file;
	String[] paths;
	LinkedList filesList = new LinkedList();
	LinkedList matchedFilesList = new LinkedList();
	LinkedList<String> processFileList = new LinkedList<String>();
	Timer timer;
	long timed;
	Map<String, String> transferMetaData = new HashMap();

	public MyTimerTask(Timer timer, long timed) {
		this.timer = timer;
		this.timed = timed;
	}

	public static void timerAccess(Map<String, String> metaDataMap) {
		metaDataMap1 = metaDataMap;

		Timer timer = new Timer();
		int time = Integer.parseInt(metaDataMap.get("pollInterval"));
		String units = metaDataMap.get("pollUnits");
		long timed = 0;
		switch (units) {
		case "minutes":
			timed = time * 60 * 1000;
			break;

		case "seconds":
			timed = time * 1000;
			break;

		case "hours":
			timed = time * 60 * 60 * 1000;
			break;

		case "days":
			timed = time * 60 * 60 * 24 * 1000;
			break;
		}

		timer.scheduleAtFixedRate(new MyTimerTask(timer, timed), 1000, timed);

	}

	@Override
	public void run() {

		file = new File(metaDataMap1.get("sourceDirectory"));
		System.out.println("Timer created for::" + file);
		int x = file.listFiles().length;
		System.out.println(x);
		paths = file.list();
		previousList = filesList.size();
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Long currentTime = Long.parseLong(simpledateFormat.format(timestamp));
		if (previousList != 0) {
			for (int i = 0; i < x; i++) {
				System.out.println("list size is " + previousList);
				for (int j = 0; j < previousList; j++) {

					if ((filesList.get(j)).toString().equals(paths[i].toString())) {
						System.out.println("if loop: " + (filesList.get(j)).toString());
						File file = new File(metaDataMap1.get("sourceDirectory") + paths[i].toString());
						String lastStringModified = simpledateFormat.format(file.lastModified());
						Long lastModified = Long.parseLong(lastStringModified);
						boolean b = (lastModified >= (currentTime - timed)) && (lastModified < currentTime);
						if (((lastModified >= (currentTime - timed)) && (lastModified < currentTime))) {
							continue;
						} else {

							matchedFilesList.add(paths[i]);
						}
						paths[i] = "";
					}
				}

			}
		}
		filesList.clear();
		filesList.addAll(matchedFilesList);
		matchedFilesList.clear();
		previousList = filesList.size();
		for (int i = 0; i < paths.length; i++) {
			if (!paths[i].equalsIgnoreCase("")
					&& TriggerPatternValidator.validateTriggerPattern(metaDataMap1.get("triggerPattern"), paths[i])) {
				filesList.add(paths[i]);
			}
		}
		if (previousList < filesList.size()) {
			System.out.println(filesList);
			int count = (filesList.size() - (filesList.size() - previousList));
			for (int i = count; i < filesList.size(); i++) {
				System.out.println(filesList.get(i));
				if (TriggerPatternValidator.validateTriggerPattern(metaDataMap1.get("triggerPattern"),
						filesList.get(i).toString())) {
					processFileList.add((filesList.get(i)).toString());
				}
			}
		}
		if (processFileList.size() > 0) {
			ExecutorService pool = Executors.newFixedThreadPool(processFileList.size());
			for (String file : processFileList) {
				System.out.println(file);
				String sourceFile = metaDataMap1.get("sourceDirectory") + "\\" + file;
				Session session = DBOperations.connectCassandra();
				String transferId = UniqueIDTest.generate();
				System.out.println(transferId);
				transferMetaData.put("transferId", transferId);
				transferMetaData.put("sourceFile", sourceFile);
				transferMetaData.put("destinationFile", "");
				DBOperations.started(session, metaDataMap1.get("monitorName"));
				DBOperations.transferDetails(session, metaDataMap1, transferMetaData);
				DBOperations.transferEventDetails(session, metaDataMap1, transferMetaData);
				session.close();
				pool.execute(new WorkerThread(metaDataMap1, transferMetaData));

			}
		}
		processFileList.clear();

	}

}
