package com.ofte.file.services;

import java.util.Map;

import org.I0Itec.zkclient.ZkClient;

import kafka.utils.ZkUtils;

public class WorkerThread implements Runnable {

	public Map<String, String> metaDataMap;
	public Map<String, String> transferMetaData;
	public ZkClient zkClient;
	public ZkUtils zkUtils;
	public String sourceFileName;

	public WorkerThread(String sourceFileName, ZkClient zkClient,
			ZkUtils zkUtils, Map<String, String> metaDataMap,
			Map<String, String> transferMetaData) {

		this.sourceFileName = sourceFileName;
		this.zkClient = zkClient;
		this.zkUtils = zkUtils;
		this.metaDataMap = metaDataMap;
		this.transferMetaData = transferMetaData;
	}

	public void run() {
		// String sPath = transferMetaData.get("sourceFileName").toString();
		// System.out.println("Entered into thread " + sPath);
		FilesProcessorService filesProcessorService = new FilesProcessorService();

		filesProcessorService.getMessages(sourceFileName, zkClient, zkUtils,
				metaDataMap, transferMetaData);

		// Lock lock = new ReentrantLock();
		// FilesProcessor fileProcessor = new FilesProcessor();
		// lock.lock();
		// fileProcessor.getMessages(sPath, metaDataMap, transferMetaData);
		System.out.println("fileProcessor releasing");
		// lock.unlock();
	}

}
