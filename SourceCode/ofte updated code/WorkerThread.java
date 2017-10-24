package com.ofte.services;

import java.util.Map;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;

public class WorkerThread implements Runnable {

	public Map<String, String> metaDataMap;
	public Map<String, String> transferMetaData;

	public WorkerThread(Map<String, String> metaDataMap, Map<String, String> transferMetaData) {

		// this.sPath = file;
		this.metaDataMap = metaDataMap;
		this.transferMetaData = transferMetaData;
	}

	public void run() {
		String sPath = transferMetaData.get("sourceFile").toString();
		System.out.println("Entered into thread " + sPath);
//		Lock lock = new ReentrantLock();
		FilesProcessorService fileProcessor = new FilesProcessorService();
//		lock.lock();
		fileProcessor.getMessages(sPath, metaDataMap, transferMetaData);
		System.out.println("fileProcessor releasing");
//		lock.unlock();
	}

}
