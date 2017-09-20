package timer;

//import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TriggerPatternValidator {

  public static boolean validateTriggerPattern(String triggerPattern, String fileName) {
	   boolean validatedResult = true;
       //String fileName = "fdsdfgsabcdefkjdashf";
       int len = fileName.length();
       //String triggerPattern = "fdsdfgskjdashf";
       int index ;
       
       if (triggerPattern.contains("*") || triggerPattern.contains("?")) {
    	   if (triggerPattern.contains("*")) {
    		   index = triggerPattern.lastIndexOf("*");
    		   if (!fileName.contains(triggerPattern.substring(0, index))) {
    			   validatedResult= false;
    		   }
    		   if (!fileName.contains(triggerPattern.substring(index+1))) {
    			   validatedResult= false;
    		   }
    	   }
    	   if ( triggerPattern.contains("?")) {
    		   index = triggerPattern.lastIndexOf("?");
    		   triggerPattern.indexOf("?");
    		   if (!fileName.contains(triggerPattern.substring(0, triggerPattern.indexOf("?")))) {
    			   validatedResult= false;
    		   }
    		   if (!fileName.contains(triggerPattern.substring(index+1))) {
    			   validatedResult= false;
    		   }
    		   if (!(triggerPattern.length() == len)) {
    			   validatedResult = false;
    		   }
    		   
    	   }
       }else {
       Pattern p = Pattern.compile(triggerPattern);
       
	     Matcher m = null;
       int i=0;
      while(i<len) {
    	 char s1 = fileName.charAt(i);
    	     String s2 = Character.toString(s1);
          m = p.matcher(s2);
	      validatedResult = m.find();
	      if (validatedResult==false)
	    	  {break;}
	     i++;
      
	    
      }}
       return validatedResult;  
	     
  }
}