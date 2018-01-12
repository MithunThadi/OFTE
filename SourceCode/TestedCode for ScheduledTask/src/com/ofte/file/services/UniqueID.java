package com.ofte.file.services;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.UUID;
/**
 * 
 * Class Functionality:
 * 						The functionality of this class is to generate the UniqueID for each and every file transfer.
 * 
 * Methods:
 * 			public String generateUniqueID()
 * 			
 *
 */
public class UniqueID {
	/**
	 * This method generates a random unique id for every file transfer
	 * @return UniqueID
	 */
  public String generateUniqueID() {
	//Creating an object for SimpleDateFormat class
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmssMs");
		//Creating an object for Timestamp class
		Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
		//Declaration of parameter timeStamp
		String timeStamp = sdf.format(currentTimeStamp);
		//Declaration of parameter randomNumber
		String randomNumber = UUID.randomUUID().toString();
		//Declaration of parameter randomNumArray
		String[] randomNumArray = randomNumber.split("-");
		//return statement
		return (randomNumArray[0] + randomNumArray[1] + randomNumArray[2] + randomNumArray[3] + timeStamp + randomNumArray[4]);
	}
}