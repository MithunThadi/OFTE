package com.ofte.services;

import java.io.File;
import java.io.IOException;
import java.util.Map;
/**
 * 
 * Class Functionality: The functionality of this class is to delete the files
 * that are transfered successfully to the destination directory from the source
 * directory only when sourceDisposition value is equal to delete and then
 * generates an acknowledgement. Methods: public void acknowledge(Map<String,
 * String> transferMetaData,Map<String, String> metadata)
 * 
 * 
 *
 */
public class Acknowledgement {
	/**
	 * This method first connects to cassandra and gets the required values.If
	 * transferstatus equals to success and sourceDisposition equals to delete
	 * then it deletes the files from source directory
	 * 
	 * @param transferMetaData
	 * @param metaData
	 */
	public void acknowledge(Map<String, String> transferMetaData,
			Map<String, String> metaData) {
		// Creation of CassandraInteracter object
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		// Declaration of parameter transfer status and initialising it by
		// connecting to cassandra and fetching transferStatusCheck from
		// transferMetaData
		String transferStatus = cassandraInteracter.transferStatusCheck(
				cassandraInteracter.connectCassandra(), transferMetaData);
		System.out.println(transferStatus);
		// Declaration of parameter sourceDisposition and initialising it by
		// getting sourceDisposition from metadata
		String sourceDisposition = metaData.get("sourceDisposition");
		System.out.println(sourceDisposition);
		// Declaration of parameter sourceFile and initialising it by connecting
		// to cassandra and fetching getSourceFilePath from transferMetaData
		String sourceFile = cassandraInteracter.getSourceFilePath(
				cassandraInteracter.connectCassandra(), transferMetaData);
		System.out.println(sourceFile);
		// if loop to check sourceDisposition not equals to null
		if (!metaData.get("destinationTriggerPattern").toString()
				.equalsIgnoreCase(metaData.get("destinationFilePattern"))) {
			VariablesSubstitution variablesSubstitution = new VariablesSubstitution();

			String destinationTriggerFile = transferMetaData
					.get("destinationFile")
					.substring(0,
							transferMetaData.get("destinationFile")
									.lastIndexOf('.'))
					.concat((variablesSubstitution.variableSubstitutor(
							transferMetaData,
							metaData.get("destinationTriggerPattern")))
									.replace("*", ""));
			System.out.println(destinationTriggerFile);
			File destinationFileTrigger = new File(destinationTriggerFile);
			try {
				destinationFileTrigger.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		if (sourceDisposition != null
				&& sourceDisposition.equalsIgnoreCase("delete")) {
			// if loop to check transferStatus and sourceDisposition
			if (transferStatus.equalsIgnoreCase("success")
					&& sourceDisposition.equalsIgnoreCase("delete")) {
				System.out.println("Congrats.........");
				// Creating an object for File class
				File file = new File(sourceFile);
				// Deleting a file
				file.delete();
				System.out.println(file.getName() + " is Deleted");
			}

		}

	}
}
