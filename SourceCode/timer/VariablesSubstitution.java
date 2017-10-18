package commm;

import java.util.Map;

public class VariablesSubstitution {

	public static String variableSubstitutor(Map<String, String> transferMetaDataMap, String sourceFilePattern){ 
		// public static MetadataMap variableSubstitutor(MetaDataMap
		// transferMetaDataMap){

		String FileName = transferMetaDataMap.get("FileName");
		String FilePath = transferMetaDataMap.get("FilePath");
		
		for (int i = 0; i < sourceFilePattern.length(); i++) {
			if (sourceFilePattern.charAt(i) == '#') {
				//
				switch (sourceFilePattern.substring(i + 2, sourceFilePattern.indexOf("("))) {
				case "FileName":
					sourceFilePattern = replacer(sourceFilePattern, FileName, i);
					System.out.println(sourceFilePattern);
					break;
				case "FilePath":
					sourceFilePattern = replacer(sourceFilePattern, FilePath, i);
					break;
				default:
					break;
				}
			}

		}
		System.out.println(sourceFilePattern);
		return sourceFilePattern;
		
	}

	public static String replacer(String sourceFilePattern, String fileNameORfilePath, int i) {
		String delimiter = sourceFilePattern.substring(sourceFilePattern.indexOf("(") + 1, sourceFilePattern.indexOf("(") + 2);
		if (delimiter.equalsIgnoreCase(".")) {
			delimiter = "\\.";
		} else if (delimiter.equalsIgnoreCase("\\")) {
			delimiter = "\\\\";
		}
		int tokenValue = Integer.parseInt(sourceFilePattern.substring(sourceFilePattern.indexOf("(") + 3, sourceFilePattern.indexOf("(") + 4));
		String fileArray[] = fileNameORfilePath.split(delimiter);
		System.out.println(sourceFilePattern.substring(i, sourceFilePattern.indexOf("}") + 1) + " " + fileArray[tokenValue - 1]);
		return sourceFilePattern.replace(sourceFilePattern.substring(i, sourceFilePattern.indexOf("}") + 1), fileArray[tokenValue - 1]);
	}
}
