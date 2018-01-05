package com.ofte.services;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
/**
 * 
 * Class Functionality: This class has methods to connect and interact with
 * Cassandra DB according to OFTE requirements Methods: public String
 * transferStatusCheck(Session session, Map<String, String> transferMetaData2)
 * public String getSourceFilePath(Session session, Map<String, String>
 * transferMetaData2) public void starting(Session session, String Monitor_name)
 * public void started(Session session,String Monitor_name) public void
 * stopped(Session session, String Monitor_name) public void deleting(Session
 * session, String Monitor_name) public void deletingThread(Session session,
 * String Monitor_name) public void deleted(Session session, String
 * Monitor_name) public void transferDetails(Session session,Map<String,String>
 * map, Map<String, String> transferMetaData) public void
 * updateTransferDetails(Session session, Map<String, String> transferMetaData1,
 * Map<String, String> metadata) public void transferEventDetails(Session
 * session, Map<String, String> metadata1, Map<String, String> transferMetaData)
 * public void updateTransferEventPublishDetails(Session session, Map<String,
 * String> transferMetaData1) public void
 * updateTransferEventConsumeDetails(Session session,Map<String, String>
 * transferMetaData1) public String DBMonitorCheck(Session session, String
 * Monitor_name)throws NoSuchFieldException, SecurityException public String
 * kafkaSecondCheckMonitor(Session session, String Monitor_name)throws
 * NoSuchFieldException, SecurityException public String
 * kafkaSecondCheckTransfer(Session session, String transfer_id)throws
 * NoSuchFieldException, SecurityException public Session connectCassandra()
 */
public class CassandraInteracter {
	// Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	// Creating an object for Timestamp class
	Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
	/**
	 * This method retrieves the transfer status for a particular transfer id
	 * 
	 * @param session
	 * @param transferMetaData2
	 * @return transferStatus
	 */
	public String transferStatusCheck(Session session,
			Map<String, String> transferMetaData2) {
		// Declaration of parameter transferStatus and initialising it to null
		String transferStatus = null;
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session.execute(
				"select transfer_status from monitor_transfer where transfer_id ='"
						+ transferMetaData2.get("transferId") + "';");
		// for each loop to iterate the row
		for (Row row : result) {
			// Updating transferStatus by getting transfer_status from each row
			transferStatus = row.getString("transfer_status");
		}
		// Closing the session
		session.close();
		// return statement
		return transferStatus;
	}

	/**
	 * This method retrieves the source File path for a particular transfer id
	 * 
	 * @param session
	 * @param transferMetaData2
	 * @return sourceFile
	 */
	public String getSourceFilePath(Session session,
			Map<String, String> transferMetaData2) {
		// Declaration of parameter sourceFile and initialising it to null
		String sourceFile = null;
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session.execute(
				"select source_file from monitor_transfer where transfer_id ='"
						+ transferMetaData2.get("transferId") + "';");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			sourceFile = row.getString("source_file");
		}
		session.close();
		// return statement
		return sourceFile;
	}

	public HashMap<String, String> getRowDetails(Session session,
			String transferId) {
		// Declaration of parameter sourceFile and initialising it to null
		// String transferDetails = null;
		HashMap<String, String> map = new HashMap<String, String>();
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session.execute(
				"select job_name,source_file,target_file from monitor_transfer where transfer_id ='"
						+ transferId + "';");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			// transferDetails = row.getString("job_name") + "," +
			// row.getString("source_file") + "," +
			// row.getString("target_file");
			map.put("job_name", row.getString("job_name"));
			map.put("source_file", row.getString("source_file"));
			map.put("target_file", row.getString("target_file"));
		}
		session.close();
		// return statement
		return map;
	}

	/**
	 * This method inserts the values into Monitor table based on monitor name
	 * 
	 * @param session
	 * @param Monitor_name
	 */
	public void starting(Session session, String Monitor_name,String pollTime) {
		// Inserting the values into Monitor table
		session.execute(
				"INSERT INTO Monitor(monitor_name,thread_status,monitor_status,current_timestamp,poll_time) VALUES ('"
						+ Monitor_name + "','creating','starting','" + timeStamp
						+ "' ,'"+pollTime+"');");
		// Closing the session
		session.close();
	}

	/**
	 * This method updates the thread status and monitor status values in the
	 * monitor table based on monitor name
	 * 
	 * @param session
	 * @param Monitor_name
	 */
	public void started(Session session, String Monitor_name, String pollTime) {
		// Updating the thread_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET thread_status = 'started' where monitor_name = '"
						+ Monitor_name + "';");
		// Updating the monitor_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET monitor_status = 'started' where monitor_name = '"
						+ Monitor_name + "';");
		session.execute("UPDATE Monitor SET poll_time = '" + pollTime
				+ "' where monitor_name = '" + Monitor_name + "';");
		// session.execute("INSERT INTO Monitor(current_timestamp) VALUES ('"
		// + timeStamp + "');");
		// Closing the session
		session.close();
	}

	/**
	 * This method updates the thread status and monitor status values in the
	 * monitor table based on monitor name
	 * 
	 * @param session
	 * @param Monitor_name
	 */
	public void stopped(Session session, String Monitor_name) {
		// Updating the thread_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET thread_status = 'stopped' where monitor_name = '"
						+ Monitor_name + "';");
		// Updating the monitor_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET monitor_status = 'stopped' where monitor_name = '"
						+ Monitor_name + "';");
		// closing session
		session.close();
	}

	/**
	 * This method updates the monitor status value in the monitor table based
	 * on monitor name
	 * 
	 * @param session
	 * @param Monitor_name
	 */
	public void deleting(Session session, String Monitor_name) {
		// Updating the monitor_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET Monitor_status = 'deleted' where monitor_name = '"
						+ Monitor_name + "';");
		// Updating the thread_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET thread_status = 'deleting' where monitor_name = '"
						+ Monitor_name + "';");
		// Closing the session
		session.close();
	}

	/**
	 * This method updates the thread status and monitor status values in the
	 * monitor table based on monitor name
	 * 
	 * @param session
	 * @param Monitor_name
	 */
	public void deletingMonitorThread(Session session, String Monitor_name) {
		// Updating the monitor_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET Monitor_status = 'deleted' where monitor_name = '"
						+ Monitor_name + "';");
		// Updating the thread_status in the Monitor table
		session.execute(
				"UPDATE Monitor SET thread_status = 'deleted' where monitor_name = '"
						+ Monitor_name + "';");
		// Closing the session
		session.close();
	}

	/**
	 * This method deletes the values in the monitor table based on monitor name
	 * 
	 * @param session
	 * @param Monitor_name
	 */
	public void deleteMonitor(Session session, String Monitor_name) {
		// Deleting the Monitor
		session.execute("DELETE FROM Monitor " + "WHERE Monitor_name = '"
				+ Monitor_name + "';");
		// deleting metadata
		session.execute("DELETE FROM Monitor_metadata "
				+ "WHERE Monitor_name = '" + Monitor_name + "';");
		// Closing the session
		session.close();
	}

	/**
	 * This method inserts the values into monitor transfer table
	 * 
	 * @param session
	 * @param map
	 * @param transferMetaData
	 */
	public void transferDetails(Session session, Map<String, String> map,
			Map<String, String> transferMetaData) {
		// Inserting the values into monitor_transfer table
		if (System.getProperty("os.name").contains("Linux")) {
			session.execute(
					"insert into monitor_transfer(job_name,source_file,transfer_id,current_timestamp) "
							+ "values(" + "'" + map.get("jobName") + "'" + ",'"
							+ transferMetaData.get("sourceFileName")
									.replace("\\", "/")
							+ "'" + ",'" + transferMetaData.get("transferId")
							+ "'" + ",'" + timeStamp + "');");
		} else if (System.getProperty("os.name").contains("Windows")) {
			session.execute(
					"insert into monitor_transfer(job_name,source_file,transfer_id,current_timestamp) "
							+ "values(" + "'" + map.get("jobName") + "'" + ",'"
							+ transferMetaData.get("sourceFileName") + "'"
							+ ",'" + transferMetaData.get("transferId") + "'"
							+ ",'" + timeStamp + "');");
		}

		session.close();
	}

	/**
	 * This method updates the target file and transfer status values in monitor
	 * transfer table based on transfer id
	 * 
	 * @param session
	 * @param transferMetaData1
	 * @param metadata
	 */
	public void updateTransferDetails(Session session,
			Map<String, String> transferMetaData1,
			Map<String, String> metadata) {
		// Updating the target_file in monitor_transfer table
		if (System.getProperty("os.name").contains("Linux")) {
			session.execute("update monitor_transfer set target_file='"
					+ transferMetaData1.get("destinationFile").replace("\\",
							"/")
					+ "' " + "where transfer_id= '"
					+ transferMetaData1.get("transferId") + "';");
		} else if (System.getProperty("os.name").contains("Windows")) {
			session.execute("update monitor_transfer set target_file='"
					+ transferMetaData1.get("destinationFile") + "' "
					+ "where transfer_id= '"
					+ transferMetaData1.get("transferId") + "';");
		}

		session.close();
	}

	public void success(Session session,
			Map<String, String> transferMetaData1) {

		// Updating the transfer_status in monitor_transfer table
		session.execute(
				"update monitor_transfer set transfer_status ='success' where transfer_id= '"
						+ transferMetaData1.get("transferId") + "';");
		session.close();

	}

	/**
	 * This method inserts the values into transfer event table
	 * 
	 * @param session
	 * @param metadata1
	 * @param transferMetaData
	 */
	public void transferEventDetails(Session session,
			Map<String, String> metadata1,
			Map<String, String> transferMetaData) {
		// Inserting the values into transfer_event table
		session.execute(
				"insert into transfer_event(transfer_id,monitor_name,current_timestamp) "
						+ "values('" + transferMetaData.get("transferId")
						+ "','" + metadata1.get("monitorName") + "'" + ",'"
						+ timeStamp + "');");
		// Closing the session
		session.close();
	}

	/**
	 * This method updates the producer key value in transfer event table
	 * 
	 * @param session
	 * @param transferMetaData1
	 */
	public void updateTransferEventPublishDetails(Session session,
			Map<String, String> transferMetaData1) {
		// Updating the producer_key in transfer_event table
		session.execute("update transfer_event set producer_key='"
				+ transferMetaData1.get("incrementPublish")
				+ "' where transfer_id ='" + transferMetaData1.get("transferId")
				+ "';");
		// Closing the session
		session.close();
	}

	/**
	 * This method updates the consumer key value in transfer event table
	 * 
	 * @param session
	 * @param transferMetaData1
	 */
	public void updateTransferEventConsumeDetails(Session session,
			Map<String, String> transferMetaData1) {
		// Updating the consumer_key in transfer_event table
		session.execute("update transfer_event set consumer_key='"
				+ transferMetaData1.get("incrementConsumer")
				+ "' where transfer_id ='" + transferMetaData1.get("transferId")
				+ "';");
		// Closing the session
		session.close();
	}

	/**
	 * This method retrieves the all the details from the monitor table
	 * 
	 * @param session
	 * @param Monitor_name
	 * @return monitortSatus
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public String DBMonitorCheck(Session session, String Monitor_name)
			throws NoSuchFieldException, SecurityException {
		// Declaration of parameter monitortSatus and initialising it to null
		String monitortSatus = null;
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session
				.execute("select * from Monitor where monitor_name='"
						+ Monitor_name + "';");
		// for each loop to iterate the row
		for (Row row : result) {
			// Updating monitortSatus by getting monitor_status from each row
			monitortSatus = row.getString("monitor_status");
		}
		// Closing the session
		session.close();
		// return statement
		return monitortSatus;
	}

	/**
	 * This method retrieves the all the details from the monitor table
	 * 
	 * @param session
	 * @param Monitor_name
	 * @return monitorAllDetails
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public String kafkaSecondCheckMonitor(Session session, String Monitor_name)
			throws NoSuchFieldException, SecurityException {
		// Declaration of parameter monitorAllDetails and initialising it to
		// null
		String monitorAllDetails = null;
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session
				.execute("select * from Monitor where monitor_name='"
						+ Monitor_name + "';");
		// for each loop to iterate the row
		for (Row row : result) {
			// Updating monitorAllDetails by getting
			// monitor_name,monitor_status,thread_status from each row
			monitorAllDetails = row.getString("monitor_name") + ","
					+ row.getString("monitor_status") + ","
					+ row.getString("thread_status");
		}
		session.close();
		// return statement
		return monitorAllDetails;
	}

	/**
	 * This method retrieves the all the details from the monitor transfer table
	 * based on transfer id
	 * 
	 * @param session
	 * @param transfer_id
	 * @return monitorTransferAllDetails
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public String kafkaSecondCheckTransfer(Session session, String transfer_id)
			throws NoSuchFieldException, SecurityException {
		// Declaration of parameter monitorTransferAllDetails and initialising
		// it to null
		String monitorTransferAllDetails = null;
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session
				.execute("select * from monitor_transfer where transfer_id='"
						+ transfer_id + "';");
		// for each loop to iterate the row
		for (Row row : result) {
			// Updating monitorTransferAllDetails by getting
			// transfer_id,transfer_id,monitor_name,source_file,target_file,transfer_status
			// from each row
			monitorTransferAllDetails = row.getString("transfer_id") + ","

					+ row.getString("source_file") + ","
					+ row.getString("target_file") + ","
					+ row.getString("transfer_status");
		}
		// Closing the session
		session.close();
		// return statement
		return monitorTransferAllDetails;
	}

	public void insertMonitorMetaData(Session session, String Monitor_name,
			String metadata) {
		// Inserting the values into Monitor table
		session.execute(
				"INSERT INTO monitor_metadata(monitor_name,metadata) VALUES ('"
						+ Monitor_name + "','" + metadata + "' );");
		// Closing the session
		session.close();
	}

	public String DBSchedulerCheck(Session session, String Scheduler_name)
			throws NoSuchFieldException, SecurityException {
		// Declaration of parameter monitortSatus and initialising it to null
		String schedulerStatus = null;
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session
				.execute("select * from Scheduler where Scheduler_name='"
						+ Scheduler_name + "';");
		// for each loop to iterate the row
		for (Row row : result) {
			// Updating monitortSatus by getting monitor_status from each row
			schedulerStatus = row.getString("scheduler_status");
		}
		// Closing the session
		session.close();
		// return statement
		return schedulerStatus;
	}
	public void schedulerStarting(Session session, String scheduler_name,String pollTime) {
		// Inserting the values into Monitor table
		session.execute(
				"INSERT INTO scheduler(scheduler_name,thread_status,scheduler_status,poll_time) VALUES ('"
						+ scheduler_name + "','creating','starting','"+pollTime+"');");
		// Closing the session
		session.close();
	}

	public void schedulerStarted(Session session, String scheduler_name) {
		// Updating the thread_status in the Monitor table
		session.execute(
				"UPDATE scheduler SET thread_status = 'started' where scheduler_name = '"
						+ scheduler_name + "';");
		// Updating the monitor_status in the Monitor table
		session.execute(
				"UPDATE scheduler SET scheduler_status = 'started' where scheduler_name = '"
						+ scheduler_name + "';");
		// Closing the session
		session.close();
	}
	public void schedulerTransferDetails(Session session,
			Map<String, String> map) {
		// Inserting the values into monitor_transfer table
		if (System.getProperty("os.name").contains("Linux")) {
			session.execute(
					"insert into monitor_transfer(job_name,source_file,transfer_id,current_timestamp) "
							+ "values(" + "'" + map.get("jobName") + "'" + ",'"
							+ map.get("sourceFileName").replace("\\", "/") + "'"
							+ ",'" + map.get("sftpTransferId") + "'" + ",'"
							+ timeStamp + "');");
		} else if (System.getProperty("os.name").contains("Windows")) {
			session.execute(
					"insert into monitor_transfer(job_name,source_file,transfer_id,current_timestamp) "
							+ "values(" + "'" + map.get("jobName") + "'" + ",'"
							+ map.get("sourceFileName").replace("\\", "/") + "'"
							+ ",'" + map.get("sftpTransferId") + "'" + ",'"
							+ timeStamp + "');");
		}
		session.close();
	}

	/**
	 * This method updates the target file and transfer status values in monitor
	 * transfer table based on transfer id
	 * 
	 * @param session
	 * @param transferMetaData1
	 * @param metadata
	 */
	public void updateSchedulerTransferDetails(Session session,
			Map<String, String> metadata) {
		// Updating the target_file in monitor_transfer table
		if (System.getProperty("os.name").contains("Windows")) {
			session.execute("update monitor_transfer set target_file='"
					+ metadata.get("destinationFile") + "' "
					+ "where transfer_id= '" + metadata.get("sftpTransferId")
					+ "';");
		} else if (System.getProperty("os.name").contains("Linux")) {
			session.execute("update monitor_transfer set target_file='"
					+ metadata.get("destinationFile").replace("\\", "/") + "' "
					+ "where transfer_id= '" + metadata.get("sftpTransferId")
					+ "';");
		}
		// Updating the transfer_status in monitor_transfer table
		session.execute(
				"update monitor_transfer set transfer_status ='success' where transfer_id= '"
						+ metadata.get("sftpTransferId") + "';");
		// Closing the session
		session.close();
	}

	public void insertScheduleMetaData(Session session, String Schedulename,
			String metadata) {
		// Inserting the values into Monitor table
		session.execute(
				"INSERT INTO scheduler_metadata(scheduler_name,metadata) VALUES ('"
						+ Schedulename + "','" + metadata + "' );");
		// Closing the session
		session.close();
	}
	public void deletingSchedulerThread(Session session, String Sheduler_name) {
		// Updating the monitor_status in the Monitor table
		session.execute(
				"UPDATE Scheduler SET Scheduler_status = 'deleted' where Scheduler_name = '"
						+ Sheduler_name + "';");
		// Updating the thread_status in the Monitor table
		session.execute(
				"UPDATE Scheduler SET thread_status = 'deleted' where Scheduler_name = '"
						+ Sheduler_name + "';");
		// Closing the session
		session.close();
	}

	/**
	 * This method deletes the values in the monitor table based on monitor name
	 * 
	 * @param session
	 * @param Monitor_name
	 */
	public void deleteScheduler(Session session, String Sheduler_name) {
		// Deleting the Monitor
		session.execute("DELETE FROM Scheduler " + "WHERE Sheduler_name = '"
				+ Sheduler_name + "';");
		// deleting metadata
		session.execute("DELETE FROM Scheduler_metadata "
				+ "WHERE Sheduler_name = '" + Sheduler_name + "';");
		// Closing the session
		session.close();
	}

	public String getSourceFile(Session session, String transferId) {
		// Declaration of parameter sourceFile and initialising it to null
		String sourceFile = null;
		// HashMap<String, String> map = new HashMap<String, String>();
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session.execute(
				"select source_file from monitor_transfer where transfer_id ='"
						+ transferId + "';");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			sourceFile = row.getString("source_file");
			// map.put("source_file", row.getString("source_file"));
		}
		session.close();
		// return statement
		return sourceFile;
	}

	public String getMonitorName(Session session, String transferId) {
		// Declaration of parameter sourceFile and initialising it to null
		String monitorName = null;
		// Declaration of parameter result which holds the row by row data of
		// the select statement
		ResultSet result = session.execute(
				"select monitor_name from transfer_event where transfer_id ='"
						+ transferId + "';");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			monitorName = row.getString("monitor_name");
		}
		session.close();
		// return statement
		return monitorName;
	}

	// public String getMonitorMetadata(Session session, String monitorName) {
	// // Declaration of parameter sourceFile and initialising it to null
	// String monitorMetadata = null;
	// // Declaration of parameter result which holds the row by row data of
	// // the select statement
	// ResultSet result = session.execute(
	// "select metadata from monitor_metadata where monitor_name ='"
	// + monitorName + "';");
	// // for loop to increment the row
	// for (Row row : result) {
	// // Updating sourceFile by getting source_file from each row
	// monitorMetadata = row.getString("metadata");
	// }
	// session.close();
	// // return statement
	// return monitorMetadata;
	// }
	public List getListMonitors(Session session) {
		List list = new LinkedList();
		ResultSet result = session.execute("select monitor_name from monitor;");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			list.add(row.getString("monitor_name"));
		}
		// System.out.println(result.);
		session.close();
		return list;
	}

	public List getListSchedulers(Session session) {
		LinkedList<String> list = new LinkedList<String>();
		ResultSet result = session
				.execute("select scheduler_name from scheduler;");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			list.add(row.getString("scheduler_name"));
		}
		// System.out.println(result.);
		session.close();
		return list;
	}
	public String getMonitorMetaData(Session session, String monitorName) {
		String monitorMetaData = null;
		ResultSet result = session.execute(
				"select metadata from monitor_metadata where monitor_name ='"
						+ monitorName + "';");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			monitorMetaData = row.getString("metadata");
		}
		session.close();
		return monitorMetaData;
	}

	public String getSchedulerMetaData(Session session, String schedulerName) {
		String schedulerMetaData = null;
		ResultSet result = session.execute(
				"select metadata from scheduler_metadata where scheduler_name ='"
						+ schedulerName + "';");
		// for loop to increment the row
		for (Row row : result) {
			// Updating sourceFile by getting source_file from each row
			schedulerMetaData = row.getString("metadata");
		}
		session.close();
		return schedulerMetaData;
	}

	/**
	 * This method used to connect the cassandra cluster and returns the session
	 * 
	 * @return session
	 */
	public Session connectCassandra() {
		// Declaration of parameter serverIp and initialising it by using
		// loadProperties file
		// InetAddress ipAddr = null;
		// try {
		// ipAddr = InetAddress.getLocalHost();
		// } catch (UnknownHostException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// String serverIp = ipAddr.getHostAddress();
		// System.out.println(serverIp);
		// if (System.getProperty("os.name").contains("Linux")) {
		// serverIp = loadProperties.getCassandraProperties()
		// .getProperty("SERVERIP");
		// } else if (System.getProperty("os.name").contains("Windows")) {
		// serverIp = loadProperties.getCassandraProperties()
		// .getProperty("SERVERIP");
		// }
		// Declaration of parameter keyspace and initialising it by using
		// loadProperties file
		String keyspace = loadProperties.getCassandraProperties()
				.getProperty("KEYSPACE");
		// Declaration of parameter session
		Session session = null;
		// Creation of Cluster object
		Cluster cluster = Cluster.builder().addContactPoints("192.168.1.226")
				.build();
		session = cluster.connect(keyspace);
		// return statement
		return session;
	}
}