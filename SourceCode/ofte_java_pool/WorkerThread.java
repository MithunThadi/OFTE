package ofte;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WorkerThread implements Runnable {

	// static String sPath1;

	public String sPath;
	public Map<String, String> metaDataMap;
	public Map<String, String> transferMetaData;

	public WorkerThread(String file, Map<String, String> metaDataMap, Map<String, String> transferMetaData) {

		this.sPath = file;
		this.metaDataMap = metaDataMap;
		this.transferMetaData = transferMetaData;
	}

	public void run() {

		System.out.println("Entered into thread " + sPath.toString());
		Lock lock = new ReentrantLock();
		FilesProcessor fileProcessor = new FilesProcessor();
		lock.lock();
		fileProcessor.getMessages(sPath, metaDataMap, transferMetaData);

		System.out.println("fileProcessor releasing");
		lock.unlock();
	}

}
