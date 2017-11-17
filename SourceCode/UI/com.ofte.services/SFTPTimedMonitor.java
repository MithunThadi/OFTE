package com.ofte.services;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SFTPTimedMonitor extends TimerTask {

	long pollTime;
	static String schedulerName;
	KafkaMapData kafkaMapData = new KafkaMapData();
	CassandraInteracter cassandraInteracter = new CassandraInteracter();
	Timer timer = new Timer();
	public void timerAccess(String string, String interval, String pollUnits) {
		try {
			schedulerName = string;
			// Creating an object for Timer class
			// Timer timer = new Timer();

			// Initialising pollInterval by getting the pollInterval from
			// metaDataMap
			int pollInterval = Integer.parseInt(interval);
			// Initialising pollTime to zero
			long pollTime = 0;
			switch (pollUnits) {
				// Depending upon the case we are setting the values into
				// metaDataMap
				case "minutes" :
					pollTime = pollInterval * 60 * 1000;
					break;
				case "seconds" :
					pollTime = pollInterval * 1000;
					break;
				case "hours" :
					pollTime = pollInterval * 60 * 60 * 1000;
					break;
				case "days" :
					pollTime = pollInterval * 60 * 60 * 24 * 1000;
					break;
			}

			// metaDataMap1.put("pollTime", String.valueOf(pollTime));
			// Watching the directory at scheduled time interval

			timer.scheduleAtFixedRate(new SFTPTimedMonitor(), 1000, pollTime);
		}
		// catching the exception for NumberFormatException
		catch (NumberFormatException numberFormatException) {
			// numberFormatException.printStackTrace(new
			// PrintWriter(log4jStringWriter));
			// logging the exception for NumberFormatException
			// logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION")
			// + log4jStringWriter.toString());
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		if (Thread.currentThread().getName().equalsIgnoreCase("Timer-0")) {
			Thread.currentThread().setName(schedulerName);
		}
		// have to update the code to check delete status in cassandra
		try {
			String schedulerStatus = cassandraInteracter.DBMonitorCheck(
					cassandraInteracter.connectCassandra(),
					Thread.currentThread().getName());
			if (schedulerStatus == "deleted") {
				Thread.currentThread().destroy();
				timer.cancel();
				// cassandraInteracter.deletingThread(cassandraInteracter.connectCassandra(),
				// metaDataMap.get("monitorName"));
				// System.exit(0);
			}
		} catch (NoSuchFieldException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (SecurityException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		// Creating of Map object
		Map<String, String> metaDataMap = new HashMap<String, String>();
		// Declaration of parameter mapData and initialising
		String mapData = kafkaMapData
				.consume("Scheduler_MetaData_" + schedulerName);
		// Declaration of parameter mapDataArrays and initialising it with
		// map values
		String[] mapDataArrays = mapData.split(",");
		// for loop to put the values into Map object
		for (int j = 0; j < mapDataArrays.length; j++) {
			metaDataMap.put(
					(mapDataArrays[j].substring(0,
							(mapDataArrays[j].indexOf("=")))).toString(),
					((mapDataArrays[j]
							.substring(mapDataArrays[j].indexOf("=") + 1)))
									.toString());

		}

		SFTPOperations sftpOperations = new SFTPOperations();
		Session session = sftpOperations.sftpConnection(
				metaDataMap.get("userName"), metaDataMap.get("password"),
				metaDataMap.get("hostIp"));
		System.out.println("entered in SFTP timed monitor");

		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		cassandraInteracter.schedulerStarted(
				cassandraInteracter.connectCassandra(),
				metaDataMap.get("schedulerName"));

		boolean sftpAsSource = false;
		boolean sftpAsDestination = false;
		LinkedList<String> sftpFilesToProcess = null;
		LinkedList<String> filesToUpload = null;
		if (metaDataMap.get("sftpAsSource") != null) {
			try {
				System.out.println("entered in sftp as source in run method");
				System.out.println(metaDataMap.get("sftpAsSource"));
				// single time triggering code and return processfileslist
				SFTPSingleTime sftpSingleTime = new SFTPSingleTime();

				sftpFilesToProcess = sftpSingleTime.singleTimeTrigger(session,
						metaDataMap.get("sftpAsSource"), metaDataMap);
				// process file list send to downloadfile method

				// sftpOperations.downloadFile(session,
				// metaDataMap1.get("sftpAsSource"), metaDataMap1);
				sftpAsSource = true;
			} catch (IOException | SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (metaDataMap.get("sftpAsDestination") != null) {

			System.out.println("enyeterd in sftp as destination in run method");

			// single time triggering code and return processfileslist
			LocalSingleTimeTrigger localSingleTimeTrigger = new LocalSingleTimeTrigger();
			filesToUpload = localSingleTimeTrigger.singleTimeTrigger(
					metaDataMap.get("schedulerName"), pollTime);
			sftpAsDestination = true;
			// process file list send to upload method

			// sftpOperations.uploadFile(session,
			// metaDataMap1.get("sftpAsDestination"), metaDataMap1);

		}
		if (sftpAsSource) {

			sftpOperations.downloadFile(metaDataMap.get("sftpAsSource"),
					metaDataMap.get("destinationDirectory"), session,
					sftpFilesToProcess);
			sftpFilesToProcess.clear();
		}
		if (sftpAsDestination) {
			sftpOperations.uploadFile(session,
					metaDataMap.get("sftpAsDestination"), metaDataMap,
					filesToUpload);

		}

	}

}
