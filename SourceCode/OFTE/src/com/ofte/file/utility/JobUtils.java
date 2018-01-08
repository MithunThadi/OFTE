package com.ofte.file.utility;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class JobUtils {
	public int createJob(String taskName, String pollInterval,String pollUnits,String name) throws IOException, InterruptedException {
//		List<String> commands = new ArrayList<String>();
		Process processTask = null;
		System.out.println("entered create Job ");
		
		String taskFileName = jobFileNameCreator(taskName,pollInterval,pollUnits,name);

			System.out.println("Job file name is "+taskFileName);
//			StringBuffer buffer = new StringBuffer();
			try {
				Process process = Runtime.getRuntime().exec("chmod 777 "+taskFileName);
				process.waitFor();
				processTask = Runtime.getRuntime().exec(taskFileName);
				processTask.waitFor();
			}catch(Exception e) {
				e.printStackTrace();
			}
		
		return processTask.exitValue();// 0 : OK 1 : Error
	}
	public String jobFileNameCreator(String taskName,String pollInterval,String pollUnits,String name) throws IOException, InterruptedException {
		String taskFileName = null;
		 
			if (!taskName.isEmpty()) {
//				taskFileName = "/ofte/cronjob/jobfiles/"+taskName+".sh";
				String fileName = createJobFile(taskName);
				taskFileName = "/ofte/cronjob/"+taskName+".sh";
				String shellCommand = null ;
				switch (pollUnits) {
				case "seconds":
					shellCommand ="(crontab -l ; echo \"*/"+pollInterval+" * * * * "+fileName+"\") |crontab -";
					break;
				case "minutes":
					shellCommand ="(crontab -l ; echo \""+pollInterval+" * * * * "+fileName+"\") |crontab -\n";
					break;
				case "hours":
					shellCommand ="(crontab -l ; echo \"0 */"+pollInterval+" * * * "+fileName+"\") |crontab -";
					break;
				case "days":
					shellCommand ="(crontab -l ; echo \"30 2 "+pollInterval+" * * "+fileName+"\") |crontab -";
					break;
				case "month":
					shellCommand ="(crontab -l ; echo \"30 2 "+pollInterval+" * * "+fileName+"\") |crontab -";
					break;
				case "weeks":
					shellCommand ="(crontab -l ; echo \"30 2 "+pollInterval+" * * "+fileName+"\") |crontab -";
					break;
				default:
					break;
				}
				System.out.println(shellCommand);
				FileWriter fileWriter = new FileWriter(new File(taskFileName));
//				switch (pollUnits) {
//				case "seconds":
//					fileWriter.write("(crontab -l ; echo \"*/"+pollInterval+" * * * * "+fileName+"\") |crontab -");
//					break;
//				case "minutes":
//					fileWriter.write("(crontab -l ; echo \""+pollInterval+" * * * * "+fileName+"\") |crontab -");
//					break;
//				case "hours":
//					fileWriter.write("(crontab -l ; echo \"0 */"+pollInterval+" * * * "+fileName+"\") |crontab -");
//					break;
//				case "days":
//					fileWriter.write("(crontab -l ; echo \"30 2 "+pollInterval+" * * "+fileName+"\") |crontab -");
//					break;
//				case "month":
//					fileWriter.write("(crontab -l ; echo \"30 2 "+pollInterval+" * * "+fileName+"\") |crontab -");
//					break;
//				case "weeks":
//					fileWriter.write("(crontab -l ; echo \"30 2 "+pollInterval+" * * "+fileName+"\") |crontab -");
//					break;
//				default:
//					break;
//				}
				fileWriter.write(shellCommand);
				fileWriter.close();
			}
		
		return taskFileName;
	}
	public String createJobFile(String taskName) throws IOException, InterruptedException {
		String jobFileName ="/ofte/cronjob/jobfiles/"+taskName+".sh";
		FileWriter fileWriter = new FileWriter(new File(jobFileName));
		fileWriter.write("#!/bin/sh\n java -cp /ofte/Executable_jar/OFTE.jar com.ofte.services.ofteprocessor "+taskName+"\n");
		fileWriter.close();
		Process process = Runtime.getRuntime().exec("chmod 777 "+jobFileName);
		process.waitFor();
		return jobFileName;
	}

}
