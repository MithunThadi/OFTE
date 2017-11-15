package com.ofte.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SFTPOperations {

	public Session sftpConnection(String user, String password, String host) {

		int port = 22;
		JSch jsch = new JSch();
		Session session = null;
		try {
			session = jsch.getSession(user, host, port);
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		session.setPassword(password);
		session.setConfig("StrictHostKeyChecking", "no");
		System.out.println("Establishing Connection...");
		try {
			session.connect();
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Connection established.");
		return session;

	}
	public void uploadFile(Session session, String serverPath,
			Map<String, String> metaDataMap1,
			LinkedList<String> filesToUpload) {

		ChannelSftp channelSftp = null;
		ChannelSftp sftpChannel = null;
		try {
			sftpChannel = (ChannelSftp) session.openChannel("sftp");
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			sftpChannel.connect();
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("sftp channel opened and connected.");
		channelSftp = (ChannelSftp) sftpChannel;
		try {
			channelSftp.cd(serverPath);
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (filesToUpload.size() != 0) {
			for (int i = 0; i < filesToUpload.size(); i++) {
				File f = new File(metaDataMap1.get("sourceDirectory") + "\\"
						+ filesToUpload.get(i));
				System.out.println(f.getName());
				try {
					channelSftp.put(new FileInputStream(f), f.getName());
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SftpException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		System.out.println("File transfered successfully to host.");

	}
	public void downloadFile(String remoteFile, String destinationDirectory,
			Session session, LinkedList<String> sftpFilesToProcess) {

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

		InputStream out = null;
		if (sftpFilesToProcess.size() > 0) {
			for (String fileinRemote : sftpFilesToProcess) {
				System.out.println(fileinRemote);
				try {
					out = sftpChannel.get(remoteFile + "//" + fileinRemote);
				} catch (SftpException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				FileOutputStream fout = null;
				try {
					fout = new FileOutputStream(
							destinationDirectory + "\\" + fileinRemote);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				try {
					IOUtils.copyLarge(out, fout);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// File f = new File(remoteFile + "//" + fileinRemote);
				// f.delete();
			}
		}

		sftpChannel.disconnect();
		session.disconnect();

	}

}
