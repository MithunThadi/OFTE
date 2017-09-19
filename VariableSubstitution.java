package timer;

import java.util.Map;
import java.util.StringTokenizer;

public class VariableSubstitution {	
	
	public static void main(String []args){
		//int length;
		String source = null, destination = null;
		String delimiter = null;
		int value = 0;
		String FileName = "sampletest.doc";
		String FilePath = "D:\\abc\\def\\ghi\\sampletest1.doc";
		String s = "D:\\abc\\def\\ghi\\#{FilePath{separator=\\}{token=5}}.csv";
//		String s = "D:\\abc\\def\\ghi\\#{FilePath{token=5}}.csv";
		String variable = s.substring(s.lastIndexOf("#"));
		System.out.println(variable);
		variable=variable.replaceAll("\\{", ",");
		variable= variable.replaceAll("\\}", ",");
		variable= variable.replaceAll(",,", ",");
		System.out.println(variable);
		
		String[] splitted = variable.split(",");
		
		
		for (int i=0; i < splitted.length;i++) {
			
			
			if (splitted[i].contains("separator")) {
				delimiter = splitted[i].substring(splitted[i].lastIndexOf("=")+1);
				//System.out.println(delimiter);
				if (delimiter.equalsIgnoreCase(".")) {
					delimiter = "\\.";
				}
				if (delimiter.equalsIgnoreCase("\\")) {
					delimiter = "\\\\";
				}
				
			}
			else {
				delimiter = "\\\\";
			}
			
			
			if (splitted[i].contains("token")) {
				value = Integer.parseInt(splitted[i].substring(splitted[i].lastIndexOf("=")+1));
			}
			else {
				
			}
			
			
			switch (splitted[i]) {
			case "FileName":
				source = FileName;
				break;
			case "FilePath":
				source = FilePath;
				break;
			case "DFileName":
				destination = "DFileName";
				break;
			case "DFilePath":
				destination = "DFilePath";
				break;
			


			default:
				break;
			}
					
		System.out.println(splitted[i]);
		}
		System.out.println(source);
		//StringTokenizer st = new StringTokenizer(source,delimiter);
		//st.nextToken(delim);
		String tempFile[]= source.split(delimiter);
		for(int i= 0; i<tempFile.length; i++) {
			//System.out.println("test");
			System.out.println(tempFile[i]);
			if(delimiter.equalsIgnoreCase("\\.") && value==1) {
				System.out.println(tempFile[0]);
			}
			if(delimiter.equalsIgnoreCase("\\.") && value==2) {
				System.out.println(tempFile[1]);
			}
			
		}
		String sourceFile = tempFile[value-1];
		
		
		String n = s.substring(s.lastIndexOf("."));
		System.out.println(n);
		
		System.out.println(sourceFile +""+ n);
		
		System.out.println(variable+" "+  sourceFile);
		System.out.println(delimiter+", "+value+ " ");
		
//		String delimiter = null;
//		String value = null;
//		String var= variable.substring(variable.indexOf("{")+1,variable.lastIndexOf("}"));
//		System.out.println(var);
//		
//			if (variable.contains("separator")) {
//				 delimiter = variable.substring(variable.indexOf("separator")+10,variable.indexOf("separator")+11);
//			}
//			if (variable.contains("token")) {
//				value = variable.substring(variable.indexOf("token")+6,variable.indexOf("token")+7);
//			}
//				
//		System.out.println(delimiter);
//		
//		System.out.println(value);
//		System.out.println(s);
		
//		String s1=s.replaceAll("#siva", "siva");
//		System.out.println(s1);
		
	}
}
