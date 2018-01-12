package com.ofte.file.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.ofte.cassandra.services.CassandraInteracter;
import com.ofte.file.utility.TaskUtils;
import com.ofte.kafka.services.KafkaMapData;

public class oftescheduler {
	
	
	public static void main(String[] args) {
		KafkaMapData kafkaMapData = new KafkaMapData();
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		TaskUtils taskObject = new TaskUtils();
		String schedulerName = args[0];
		// have to update the code to check delete status in cassandra
		try {
			String schedulerStatus = cassandraInteracter.DBSchedulerCheck(
					cassandraInteracter.connectCassandra(),
					Thread.currentThread().getName());
			if (schedulerStatus.equalsIgnoreCase("paused")) {
				return;
			}
			if (schedulerStatus.equalsIgnoreCase("deleted")) {
				cassandraInteracter.deleteScheduler(
						cassandraInteracter.connectCassandra(),
						Thread.currentThread().getName());
				System.out.println("delete the scheduler");
				int delete = taskObject.deleteTask(schedulerName);
				if (delete == 0) {
					System.out.println(schedulerName+" deleted succesfully");
				}else {
					System.out.println(schedulerName+"deletion unsuccessfull");
				}

//				Thread.currentThread().sleep(pollTime);
//				Thread.currentThread().stop();

			}
		} catch (NoSuchFieldException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (SecurityException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		// code to get the scheduler metadata from cassandra
		if (metaDataMap.isEmpty()) {
			String schedulerMetaData = cassandraInteracter.getSchedulerMetaData(
					cassandraInteracter.connectCassandra(), schedulerName);
			String[] schedulerMapDataArrays = schedulerMetaData.split(",");
			// for loop to put the values into Map object
			for (int j = 0; j < schedulerMapDataArrays.length; j++) {
				metaDataMap.put(
						(schedulerMapDataArrays[j].substring(0,
								(schedulerMapDataArrays[j].indexOf("="))))
										.toString(),
						((schedulerMapDataArrays[j].substring(
								schedulerMapDataArrays[j].indexOf("=") + 1)))
										.toString());

			}
		}

		SFTPOperations sftpOperations = new SFTPOperations();
		Session session = sftpOperations.sftpConnection(
				metaDataMap.get("userName"), metaDataMap.get("password"),
				metaDataMap.get("hostIp"));
		System.out.println("entered in SFTP timed monitor");
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
				sftpOperations.downloadFile(metaDataMap, session,
						sftpFilesToProcess);
				sftpFilesToProcess.clear();
				// sftpAsSource = true;
			} catch (IOException | SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (metaDataMap.get("sftpAsDestination") != null) {

			System.out.println("enyeterd in sftp as destination in run method");
			System.out.println(metaDataMap.get("sftpAsDestination"));
			// single time triggering code and return processfileslist
			LocalSingleTimeTrigger localSingleTimeTrigger = new LocalSingleTimeTrigger();
			filesToUpload = localSingleTimeTrigger.singleTimeTrigger(
					metaDataMap.get("schedulerName"), (Integer.parseInt(taskObject.calculatePoll(metaDataMap.get("pollUnits"), metaDataMap.get("pollInterval"))) * 60 * 1000));
			sftpOperations.uploadFile(session, metaDataMap, filesToUpload);
			filesToUpload.clear();
			// sftpAsDestination = true;
			// process file list send to upload method

			// sftpOperations.uploadFile(session,
			// metaDataMap1.get("sftpAsDestination"), metaDataMap1);

		}
		// if (sftpAsSource) {
		//
		// sftpOperations.downloadFile(metaDataMap.get("sftpAsSource"),
		// metaDataMap.get("destinationDirectory"), session,
		// sftpFilesToProcess);
		// sftpFilesToProcess.clear();
		// }
		// if (sftpAsDestination) {
		// sftpOperations.uploadFile(session,
		// metaDataMap.get("sftpAsDestination"), metaDataMap,
		// filesToUpload);
		//
		// }

	
	}

}
