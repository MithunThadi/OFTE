package ofte;

public class VariablesSubstitution {

	public static void main(String args[]) {
		// public static MetadataMap variableSubstitutor(MetaDataMap
		// transferMetaDataMap){
		String source;
		String FileName = "sample.te.st.doc";
		String FilePath = "D:\\abc\\def\\ghi\\sample.test1.doc";
		String s = "#{FilePath(.,1)}\\#{FileName(.,1)}.sadfa.#{FileName(.,4)}";
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) == '#') {
				//
				switch (s.substring(i + 2, s.indexOf("("))) {
				case "FileName":
					s = replacer(s, FileName, i);
					System.out.println(s);
					break;
				case "FilePath":
					s = replacer(s, FilePath, i);
					break;
				default:
					break;
				}
			}

		}
		System.out.println(s);
		DBOperations.connectCassandra();
	}

	public static String replacer(String source, String variable, int i) {
		String delimiter = source.substring(source.indexOf("(") + 1, source.indexOf("(") + 2);
		if (delimiter.equalsIgnoreCase(".")) {
			delimiter = "\\.";
		} else if (delimiter.equalsIgnoreCase("\\")) {
			delimiter = "\\\\";
		}
		int value = Integer.parseInt(source.substring(source.indexOf("(") + 3, source.indexOf("(") + 4));
		String array[] = variable.split(delimiter);
		System.out.println(source.substring(i, source.indexOf("}") + 1) + " " + array[value - 1]);
		return source.replace(source.substring(i, source.indexOf("}") + 1), array[value - 1]);
	}
}
