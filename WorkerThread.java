package timer;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;  
import java.util.concurrent.Executors;  

public class WorkerThread implements Runnable {
	
	//static String sPath1;
	
	public String sPath;
	public Map<String,String> metaDataMap;
	public Map<String,String> transferMetaData;
     
    
    public WorkerThread(String file, Map<String, String> metaDataMap, Map<String, String> transferMetaData) {
    	
		// TODO Auto-generated constructor stub
    	this.sPath=file;  
        this.metaDataMap=metaDataMap;
        this.transferMetaData=transferMetaData;
	}

	public void run() {
		synchronized(this) {
    	System.out.println("Entered into thread " +sPath.toString());
    	
    	FilesProcessor fileProcessor = new FilesProcessor();
    	fileProcessor.getMessages(sPath, metaDataMap,transferMetaData);
		}
    }

}
