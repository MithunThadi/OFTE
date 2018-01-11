package com.ofte.file.utility;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TaskUtils {

	public String calculatePoll(String pollUnits, String interval) {
		long pollTime = 0;

		int pollInterval = Integer.parseInt(interval);
		switch (pollUnits) {
		// Depending upon the case we are setting the values into
		// metaDataMap
		case "minutes":
			pollTime = pollInterval ;
			break;
//		case "seconds":
//			pollTime = pollInterval * 1000;
//			break;
		case "hours":
			pollTime = pollInterval * 60 ;
			break;
		case "days":
			pollTime = pollInterval * 60  * 24 ;
			break;
		}
		System.out.println(pollTime);
		return String.valueOf(pollTime);
	}
	
	public int createTask(String taskName, String taskTime,String name) throws IOException, InterruptedException {
		List<String> commands = new ArrayList<String>();
		Process processTask = null;
		System.out.println("entered create Task ");
		String taskFileName = taskFileNameCreator(taskName,name);
		System.out.println("Task file name is "+taskFileName);
		commands.add("schtasks.exe");
		commands.add("/CREATE");
		commands.add("/TN");
		commands.add(taskName);
		commands.add("/TR");
		commands.add(taskFileName);
		commands.add("/SC");
		commands.add("minute");
		commands.add("/MO");
		commands.add(taskTime);
		ProcessBuilder builder = new ProcessBuilder(commands);
		processTask = builder.start();
		processTask.waitFor();
		System.out.println("create Task done");
		return processTask.exitValue();// 0 : OK 1 : Error
	}
	
	public String taskFileNameCreator(String taskName,String name) throws IOException {
		String taskFileName = null;
			if (!taskName.isEmpty()) {
				taskFileName = "F:\\OFTE_Pack\\bin\\"+taskName+".cmd";
				FileWriter fileWriter = new FileWriter(new File(taskFileName));
				if(name.contains("monitor")) {
					fileWriter.write("@echo off\nset CUR_DIR=%cd%\njava -cp F:\\OFTE_Pack\\bin\\OFTE.jar com.ofte.file.services.ofteMonitor "+taskName+"\n");
					fileWriter.close();
				}else {
					fileWriter.write("@echo off\nset CUR_DIR=%cd%\njava -cp F:\\OFTE_Pack\\bin\\OFTE.jar com.ofte.file.services.oftescheduler "+taskName+"\n");
					fileWriter.close();
				}
			}
		
		return taskFileName;
	}
//	public String createJobFile(String taskName,String name) {
//		String jobFileName ="/ofte/cronjob/jobfiles/"+taskName+".sh";
//		try {
//			FileWriter fileWriter = new FileWriter(new File(taskName));
//			if(name.contains("monitor")) {
//				fileWriter.write("java -cp /ofte/Executable_jar/OFTE.jar com.ofte.services.ofteprcessor "+taskName);
//				fileWriter.close();
//			}else {
//				fileWriter.write("java -cp /ofte/Executable_jar/OFTE.jar com.ofte.services.oftescheduler "+taskName);
//				fileWriter.close();
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return jobFileName;
//	}
	public int deleteTask(String taskName) throws IOException, InterruptedException {
		List<String> commands = new ArrayList<String>();
		commands.add("schtasks.exe");
		commands.add("/DELETE");
		commands.add("/TN");
		commands.add(taskName);
		ProcessBuilder builder = new ProcessBuilder(commands);
		Process processTask = builder.start();
		processTask.waitFor();
		return processTask.exitValue();
	}
}
