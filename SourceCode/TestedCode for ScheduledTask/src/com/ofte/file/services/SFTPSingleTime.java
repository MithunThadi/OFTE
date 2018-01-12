package com.ofte.file.services;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SFTPSingleTime {
	LinkedList<String> filesList = new LinkedList<String>();
	LinkedList<String> matchedFilesList = new LinkedList<String>();
	LinkedList<String> processFileList = new LinkedList<String>();
	int previousListSize = 0;
	SimpleDateFormat simpledateFormat = new SimpleDateFormat("ddHHmmss");
	@SuppressWarnings({"rawtypes", "unused"})
	public LinkedList<String> singleTimeTrigger(Session session,
			String remoteFile, Map<String, String> metaDataMap1)
			throws IOException, SftpException {

		ChannelSftp sftpChannel = null;

		try {
			sftpChannel = (ChannelSftp) session.openChannel("sftp");
			sftpChannel.cd(remoteFile);
		} catch (JSchException | SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			sftpChannel.connect();
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("SFTP Channel created.");

		Vector vectorFilelist = sftpChannel.ls(remoteFile);
		// String[] filesInRenote=filelist.
		String[] filesInRemoteDir = new String[vectorFilelist.size()];
		for (int i = 0; i < vectorFilelist.size(); i++) {
			filesInRemoteDir[i] = vectorFilelist.get(i).toString().substring(
					vectorFilelist.get(i).toString().lastIndexOf(" ") + 1);
		}

		vectorFilelist.removeAllElements();
		System.out.println(vectorFilelist.isEmpty());
		int numberOfFiles = filesInRemoteDir.length;
		previousListSize = filesList.size();
		System.out.println("previous list size " + previousListSize);
		// Creating an object for Timestamp class
		Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
		// Declaration of parameter currentTime and initialising it with
		// currentTimeStamp
		Long currentTime = Long
				.parseLong(simpledateFormat.format(currentTimeStamp));
		// if loop to check the condition previousListSize not equals to zero
		if (previousListSize != 0) {
			// for loop to add the file in matchedFilesList
			for (int i = 0; i < numberOfFiles; i++) {
				System.out.println("list size is " + previousListSize);
				// for loop to add the files in matchedFilesList
				for (int j = 0; j < previousListSize; j++) {
					// if loop to check the condition filesList equals to
					// filesInDirectory
					if ((filesList.get(j)).toString()
							.equals(filesInRemoteDir[i].toString())) {
						System.out.println(
								"if loop: " + (filesList.get(j)).toString());
						// Creating an object for File class and initialising it
						// with sourceDirectory by getting values from
						// metaDataMap
						File file = new File(remoteFile + "//"
								+ filesInRemoteDir[i].toString());
						// Declaration of parameter lastStringModified and
						// initialising it with lastModified time
						String lastStringModified = simpledateFormat
								.format(file.lastModified());
						// Declaration of parameter lastModified and
						// initialising it with lastStringModified time
						Long lastModified = Long.parseLong(lastStringModified);

						Long pollTime = Long
								.parseLong(metaDataMap1.get("pollTime"));

						// if loop to check the condition lastModified
						// if (((lastModified >= (currentTime - pollTime))
						// && (lastModified < currentTime))) {
						// continue;
						// } else {
						matchedFilesList.add(filesInRemoteDir[i]);
						// }
						// Replacing filesInDirectory array with no value
						filesInRemoteDir[i] = "";
					}
				}
			}
		}
		// clear filesList
		filesList.clear();
		// adding matchedFilesList to filesList
		filesList.addAll(matchedFilesList);
		// clear matchedFilesList
		matchedFilesList.clear();
		// Initialising previousListSize with filesList.size
		previousListSize = filesList.size();
		System.out.println(filesInRemoteDir.length);
		// Creating an object for TriggerPatternValidator class
		TriggerPatternValidator triggerPatternValidator = new TriggerPatternValidator();
		// for loop to add filesInDirectory to filesList
		for (int i = 0; i < filesInRemoteDir.length; i++) {
			System.out.println((!filesInRemoteDir[i].equalsIgnoreCase("")) + " "
					+ (triggerPatternValidator.validateTriggerPattern(
							metaDataMap1.get("triggerPattern"),
							filesInRemoteDir[i]) + " " + filesInRemoteDir[i]));
			// if loop to check the triggerPattern condition before adding
			// filesList
			if ((!filesInRemoteDir[i].equalsIgnoreCase(""))
					&& (triggerPatternValidator.validateTriggerPattern(
							metaDataMap1.get("triggerPattern"),
							filesInRemoteDir[i]))) {
				// Adding filesInDirectory to filesList
				filesList.add(filesInRemoteDir[i]);
				System.out.println("tpv");
			}
		}
		System.out.println(filesList.size());
		// if loop to check previousListSize and filesListsize
		if (previousListSize < filesList.size()) {
			System.out.println(filesList);
			// Initialising the parameter count
			int count = (filesList.size()
					- (filesList.size() - previousListSize));
			// for loop to check previousListSize and filesListsize to add the
			// new files in processFileList
			for (int i = count; i < filesList.size(); i++) {
				System.out.println(filesList.get(i));
				// if loop to check the condition triggerPatternValidator and
				// adding processFileList
				if (triggerPatternValidator.validateTriggerPattern(
						metaDataMap1.get("triggerPattern"),
						filesList.get(i).toString())) {
					// Adding filesList to processFileList
					processFileList.add((filesList.get(i)).toString());
					System.out.println("tpv");
				}
			}
		}

		// InputStream out = null;
		// if (processFileList.size() > 0) {
		// for (String fileinRemote : processFileList) {
		// System.out.println(fileinRemote);
		// out = sftpChannel.get(remoteFile + "//" + fileinRemote);
		// FileOutputStream fout = new FileOutputStream(
		// metaDataMap1.get("destinationDirectory") + "\\"
		// + fileinRemote);
		//
		// IOUtils.copyLarge(out, fout);
		// // File f = new File(remoteFile + "//" + fileinRemote);
		// // f.delete();
		return processFileList;
	}
}
