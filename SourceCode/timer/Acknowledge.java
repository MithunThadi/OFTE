package commm;

import java.io.File;
import java.util.Map;

public class Acknowledge {
	
	
	public static void acknowledge(Map<String, String> transferMetaData,Map<String, String> metadata) {
		String transferStatus=DBOperations.transferStatusCheck(DBOperations.connectCassandra(), transferMetaData);
		System.out.println(transferStatus);
		String sourceDisposition = metadata.get("sourceDisposition");
		System.out.println(sourceDisposition);
		
		String sourceFile = DBOperations.getSourceFilePath(DBOperations.connectCassandra(),transferMetaData );
		System.out.println(sourceFile);
		if(sourceDisposition!=null) {
			if(transferStatus.equalsIgnoreCase("success") && sourceDisposition.equalsIgnoreCase("delete") ) {
			System.out.println("Congrats.........");
			File file = new File(sourceFile);
			file.delete();
			System.out.println(file.getName()+" is Deleted");
			
			}
		}
		
	}

}
