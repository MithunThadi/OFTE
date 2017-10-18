package commm;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Monitor Methods to update statuses(Monitor Status, Thread Status.)
 * 
 *
 */
public class DBOperations {
	/**
	 * 
	 * @param session
	 */
	static Timestamp timeStamp =new Timestamp(System.currentTimeMillis());

	public static String transferStatusCheck(Session session, Map<String, String> transferMetaData2) {
		// TODO Auto-generated method stub
		String transferStatus = null;
		
//		String query123 = "select transfer_status from monitor_transfer where transfer_id ='"+transferMetaData2.get("transferId")+"';";
		ResultSet result = session.execute("select transfer_status from monitor_transfer where transfer_id ='"+transferMetaData2.get("transferId")+"';");
		for(Row row : result) {
			transferStatus = row.getString("transfer_status");
		}
		
		return transferStatus;
	}
	public static String getSourceFilePath(Session session, Map<String, String> transferMetaData2) {
		// TODO Auto-generated method stub
		
		String sourceFile = null;
		
//		String query143 = "select source_file from monitor_transfer where transfer_id ='"+transferMetaData2.get("transferId")+"';";
		ResultSet result = session.execute("select source_file from monitor_transfer where transfer_id ='"+transferMetaData2.get("transferId")+"';");
		for(Row row : result) {
			sourceFile = row.getString("source_file");
		}
		
		return sourceFile;
	}
	public static void starting(Session session, String Monitor_name) {
		// Insert status as starting,created

//		String cqlStatementC = "INSERT INTO Monitor(monitor_name,thread_status,monitor_status,current_timestamp) VALUES ('"
//				+ Monitor_name + "','creating','starting','"+timeStamp+"' );";
		session.execute("INSERT INTO Monitor(monitor_name,thread_status,monitor_status,current_timestamp) VALUES ('"
				+ Monitor_name + "','creating','starting','"+timeStamp+"' );");
		// DBOperations.started(session, Monitor_name);
	}

	public static void started(Session session,String Monitor_name) {
		// update status with started,started
//		String cqlStatementU = "UPDATE Monitor SET thread_status = 'started' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute("UPDATE Monitor SET thread_status = 'started' where monitor_name = '"
				+ Monitor_name + "';");

//		String cqlStatementU1 = "UPDATE Monitor SET monitor_status = 'started' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute("UPDATE Monitor SET monitor_status = 'started' where monitor_name = '"
				+ Monitor_name + "';");

		// session.close();
	}

	public static void stopped(Session session, String Monitor_name) {
		// update status with stopped,stopped
//		String cqlStatementU = "UPDATE Monitor SET thread_status = 'stopped' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute("UPDATE Monitor SET thread_status = 'stopped' where monitor_name = '"
				+ Monitor_name + "';");

//		String cqlStatementU1 = "UPDATE Monitor SET monitor_status = 'stopped' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute("UPDATE Monitor SET monitor_status = 'stopped' where monitor_name = '"
				+ Monitor_name + "';");
		session.close();
	}

	public static void deleting(Session session, String Monitor_name) {
		// deleting, deleted

//		String cqlStatementU = "UPDATE Monitor SET Monitor_status = 'deleted' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute("UPDATE Monitor SET Monitor_status = 'deleted' where monitor_name = '"
				+ Monitor_name + "';");

//		String cqlStatementU1 = "UPDATE Monitor SET thread_status = 'deleting' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute( "UPDATE Monitor SET thread_status = 'deleting' where monitor_name = '"
				+ Monitor_name + "';");
		// session.close();
	}

	public static void deletingThread(Session session, String Monitor_name) {
		// deleting, deleted

//		String cqlStatementU = "UPDATE Monitor SET Monitor_status = 'deleted' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute("UPDATE Monitor SET Monitor_status = 'deleted' where monitor_name = '"
				+ Monitor_name + "';");

//		String cqlStatementU1 = "UPDATE Monitor SET thread_status = 'deleted' where monitor_name = '"
//				+ Monitor_name + "';";
		session.execute("UPDATE Monitor SET thread_status = 'deleted' where monitor_name = '"
				+ Monitor_name + "';");
		// session.close();
	}
	
	public static void deleted(Session session, String Monitor_name) {
		// deleted, deleted

//		String cqlStatementD = "DELETE FROM Monitor "
//				+ "WHERE Monitor_name = '" +  Monitor_name+ "';";
		session.execute("DELETE FROM Monitor "
				+ "WHERE Monitor_name = '" +  Monitor_name+ "';");

	}
	public static void transferDetails(Session session,Map<String,String> map, Map<String, String> transferMetaData)
	{
//		System.out.println(map);
//		System.out.println(transferMetaData);
//		String cqlStatementinsert="insert into monitor_transfer(monitor_name,job_name,source_file,transfer_id,current_timestamp) "
//				+ "values("+"'"+map.get("monitorName")+"','"+map.get("jobName")+"'"+",'"+transferMetaData.get("sourceFileName").replace("\\", "/")+"'"+",'"+transferMetaData.get("transferId")+"'"+",'"+timeStamp+"');";
		session.execute("insert into monitor_transfer(monitor_name,job_name,source_file,transfer_id,current_timestamp) "
				+ "values("+"'"+map.get("monitorName")+"','"+map.get("jobName")+"'"+",'"+transferMetaData.get("sourceFileName").replace("\\", "/")+"'"+",'"+transferMetaData.get("transferId")+"'"+",'"+timeStamp+"');");
		
	}
	public static void updateTransferDetails(Session session, Map<String, String> transferMetaData1, Map<String, String> metadata)
	{
//		String cqlstatementu="update monitor_transfer set target_file='"
//	                                       +transferMetaData1.get("destinationFile").replace("\\", "/")+"' "+"where transfer_id= '" +transferMetaData1.get("transferId")+"';";
		session.execute("update monitor_transfer set target_file='"
                +transferMetaData1.get("destinationFile").replace("\\", "/")+"' "+"where transfer_id= '" +transferMetaData1.get("transferId")+"';");
//		String cqlstatementu2="update monitor_transfer set transfer_status ='success' where transfer_id= '" +transferMetaData1.get("transferId")+"';";
		session.execute("update monitor_transfer set transfer_status ='success' where transfer_id= '" +transferMetaData1.get("transferId")+"';");
	}
	public static void transferEventDetails(Session session, Map<String, String> metadata1, Map<String, String> transferMetaData) {
//		String cqlInsert="insert into transfer_event(transfer_id,monitor_name,current_timestamp) "+"values('"+transferMetaData.get("transferId")+"','"+metadata1.get("monitorName")+"'"+",'"+timeStamp+"');";
		session.execute("insert into transfer_event(transfer_id,monitor_name,current_timestamp) "+"values('"+transferMetaData.get("transferId")+"','"+metadata1.get("monitorName")+"'"+",'"+timeStamp+"');");
		
	}
	public static void updateTransferEventPublishDetails(Session session, Map<String, String> transferMetaData1) {
	
//		String cqlPublishUpdate="update transfer_event set producer_key='"+transferMetaData1.get("incrementPublish")+"' where transfer_id ='"+transferMetaData1.get("transferId")+"';";
		session.execute("update transfer_event set producer_key='"+transferMetaData1.get("incrementPublish")+"' where transfer_id ='"+transferMetaData1.get("transferId")+"';");
	}
	public static void updateTransferEventConsumeDetails(Session session,Map<String, String> transferMetaData1) {
//		String cqlConsumeUpdate="update transfer_event set consumer_key='"+transferMetaData1.get("incrementConsumer")+"' where transfer_id ='"+transferMetaData1.get("transferId")+"';";
		session.execute("update transfer_event set consumer_key='"+transferMetaData1.get("incrementConsumer")+"' where transfer_id ='"+transferMetaData1.get("transferId")+"';");
	}
	

	public static String DBMonitorCheck(Session session, String Monitor_name)
			throws NoSuchFieldException, SecurityException {
		String monitortSatus = null ;
		
//		String cqlStatementR = "select * from Monitor where monitor_name='"
//				+ Monitor_name + "';";
		ResultSet result = session.execute("select * from Monitor where monitor_name='"
				+ Monitor_name + "';");
		
		for (Row row : result) {
			
			monitortSatus=row.getString("monitor_status");

		}
		
		return monitortSatus;
	}
	public static String kafkaSecondCheckMonitor(Session session, String Monitor_name)
			throws NoSuchFieldException, SecurityException {
		String monitorAllDetails = null ;
		
//		String cqlStatementR = "select * from Monitor where monitor_name='"
//				+ Monitor_name + "';";
		ResultSet result = session.execute("select * from Monitor where monitor_name='"
				+ Monitor_name + "';");
		
		for (Row row : result) {
			
			monitorAllDetails=row.getString("monitor_name")+","+row.getString("monitor_status")+","+row.getString("thread_status");

		}
		
		return monitorAllDetails;
	}
	
	public static String kafkaSecondCheckTransfer(Session session, String transfer_id)
			throws NoSuchFieldException, SecurityException {
		String monitorTransferAllDetails = null ;
		
//		String cqlStatementR = "select * from monitor_transfer where transfer_id='"
//				+ transfer_id + "';";
		ResultSet result = session.execute("select * from monitor_transfer where transfer_id='"
				+ transfer_id + "';");
		
		for (Row row : result) {
			
			monitorTransferAllDetails=row.getString("transfer_id")+","+row.getString("job_name")+","+row.getString("monitor_name")+","+
			row.getString("source_file")+","+row.getString("target_file")+","+row.getString("transfer_status");

		}
		
		return monitorTransferAllDetails;
	}
//	public static String listTables(String tname)
//	{
//		String serverIp = "127.0.0.1";
//		String keyspace = "ofte";
//		Session session = null;
//		String allTables = null;
//		Cluster cluster = Cluster.builder().addContactPoints(serverIp).build();
//		session = cluster.connect(keyspace);
//		Metadata metadata = cluster.getMetadata();
//		Iterator<TableMetadata> tableMetaData = metadata.getKeyspace(keyspace).getTables().iterator();
//
//	      while(tableMetaData.hasNext()){
//	          TableMetadata t = tableMetaData.next();
//	          System.out.println(t.getName());
//	          if(t.getName()==tname) {
//	        	  allTables=t.getName();
//	          }
//	      }
//		return allTables;
//		
//	}
	

	// System.out.println(s);
	// System.out.println(result.getColumnDefinitions());
	// return s;

	// session.close();
	// ((DBOperations) o).connectCassandra().getCluster().close();

	public static Session connectCassandra() {
		//need to read ip and keyspace from a property file visible to user.
		String serverIp = "127.0.0.1";
		String keyspace = "ofte";
		Session session = null;
		Cluster cluster = Cluster.builder().addContactPoints(serverIp).build();
		session = cluster.connect(keyspace);
		// System.out.println("deleted successfully 00");

		return session;
	}

//	public static void main(String[] args) throws NoSuchFieldException,
//			SecurityException {
//		DBOperations dboperations = new DBOperations();
//		Session session = dboperations.connectCassandra();
//
//		
//		String r = dboperations.printResult(session, "testfds");
//		System.out.println(r+"rf");
//		session.close();
//		session.getCluster().close();
//
//		System.out.println("deleted successfully");

//	}

//}
}
