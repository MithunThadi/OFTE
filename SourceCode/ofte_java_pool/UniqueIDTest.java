package ofte;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.UUID;

public class UniqueIDTest {
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmssMs");

	public static String generate() {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		// System.out.println(timestamp);

		// method 2 - via Date
		// Date date = new Date();
		// System.out.println(sdf.format(timestamp));
		String t = sdf.format(timestamp);
		String s = UUID.randomUUID().toString();
		// System.out.println (s);
		String[] s1 = s.split("-");
		// System.out.println(s1[4]);
		return (s1[0] + s1[1] + s1[2] + s1[3] + t + s1[4]);

	}
}