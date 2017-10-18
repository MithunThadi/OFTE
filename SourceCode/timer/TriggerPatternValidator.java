//import java.util.StringTokenizer;
package commm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TriggerPatternValidator {

	public static boolean validateTriggerPattern(String triggerPattern, String fileName) {
		boolean validatedResult = true;
		// String fileName = "fdsdfgsabcdefkjdashf";
		int fileNameLength = fileName.length();
		// String triggerPattern = "fdsdfgskjdashf";
		int index;

		if (triggerPattern.contains("*") || triggerPattern.contains("?")) {
			if (triggerPattern.contains("*")) {
				index = triggerPattern.lastIndexOf("*");
				if (!fileName.contains(triggerPattern.substring(0, index))) {
					System.out.println("false 1");
					validatedResult = false;
				}
				if (!fileName.contains(triggerPattern.substring(index + 1))) {
					System.out.println("false 2");
					validatedResult = false;
				}
			}
			if (triggerPattern.contains("?")) {
				index = triggerPattern.lastIndexOf("?");
				triggerPattern.indexOf("?");
				if (!fileName.contains(triggerPattern.substring(0, triggerPattern.indexOf("?")))) {
					validatedResult = false;
					System.out.println("false 3");
				}
				if (!fileName.contains(triggerPattern.substring(index + 1))) {
					validatedResult = false;
					System.out.println("false 4");
				}
				if (!(triggerPattern.length() == fileNameLength)) {
					validatedResult = false;
					System.out.println("false 5");
				}

			}
		} else {
			Pattern pattern = Pattern.compile(triggerPattern);

			Matcher matcher = null;
			int length = 0;
			while (length < fileNameLength) {
				char characterInFileName = fileName.charAt(length);
				String character = Character.toString(characterInFileName);
				matcher = pattern.matcher(character);
				validatedResult = matcher.find();
				if (validatedResult == false) {
					break;
				}
				length++;

			}
		}
		return validatedResult;

	}
}