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
	static Map<String, String> metaDataMap1 = new HashMap<>();
	long pollTime;
	public void timerAccess(Map<String, String> metaDataMap, String interval,
			String pollUnits) {
		try {
			metaDataMap1 = metaDataMap;
			// Creating an object for Timer class
			Timer timer = new Timer();
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

			metaDataMap1.put("pollTime", String.valueOf(pollTime));
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
		SFTPOperations sftpOperations = new SFTPOperations();
		Session session = sftpOperations.sftpConnection("root", "iibwmq@2k16",
				"192.168.1.228");
		System.out.println("entered in SFTP timed monitor");

		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		cassandraInteracter.schedulerStarted(
				cassandraInteracter.connectCassandra(),
				metaDataMap1.get("schedulerName"));

		boolean sftpAsSource = false;
		boolean sftpAsDestination = false;
		LinkedList<String> sftpFilesToProcess = null;
		LinkedList<String> filesToUpload = null;
		if (metaDataMap1.get("sftpAsSource") != null) {
			try {
				System.out.println("entered in sftp as source in run method");
				System.out.println(metaDataMap1.get("sftpAsSource"));
				// single time triggering code and return processfileslist
				SFTPSingleTime sftpSingleTime = new SFTPSingleTime();

				sftpFilesToProcess = sftpSingleTime.singleTimeTrigger(session,
						metaDataMap1.get("sftpAsSource"), metaDataMap1);
				// process file list send to downloadfile method

				// sftpOperations.downloadFile(session,
				// metaDataMap1.get("sftpAsSource"), metaDataMap1);
				sftpAsSource = true;
			} catch (IOException | SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (metaDataMap1.get("sftpAsDestination") != null) {

			System.out.println("enyeterd in sftp as destination in run method");

			// single time triggering code and return processfileslist
			LocalSingleTimeTrigger localSingleTimeTrigger = new LocalSingleTimeTrigger();
			filesToUpload = localSingleTimeTrigger.singleTimeTrigger(
					metaDataMap1.get("schedulerName"), pollTime);
			sftpAsDestination = true;
			// process file list send to upload method

			// sftpOperations.uploadFile(session,
			// metaDataMap1.get("sftpAsDestination"), metaDataMap1);

		}
		if (sftpAsSource) {

			sftpOperations.downloadFile(metaDataMap1.get("sftpAsSource"),
					metaDataMap1.get("destinationDirectory"), session,
					sftpFilesToProcess);
			sftpFilesToProcess.clear();
		}
		if (sftpAsDestination) {
			sftpOperations.uploadFile(session,
					metaDataMap1.get("sftpAsDestination"), metaDataMap1,
					filesToUpload);

		}

	}

}
