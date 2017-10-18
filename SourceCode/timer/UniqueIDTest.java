package commm;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.UUID;

public class UniqueIDTest {
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmssMs");

	public static String generateUniqueID() {
		Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
		// System.out.println(timestamp);

		// method 2 - via Date
		// Date date = new Date();
		// System.out.println(sdf.format(timestamp));
		String timeStamp = sdf.format(currentTimeStamp);
		String randomNumber = UUID.randomUUID().toString();
		// System.out.println (s);
		String[] randomNumArray = randomNumber.split("-");
		// System.out.println(s1[4]);
		return (randomNumArray[0] + randomNumArray[1] + randomNumArray[2] + randomNumArray[3] + timeStamp + randomNumArray[4]);

	}
}