package timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.HashMap;
import java.util.Map;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.naming.directory.InvalidAttributesException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;

import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;

public class XMLFileCreation {
	//public static String xmlFilePath="D:\\xmlfile2.xml" ;
	
	public static void access(Map<String,String> metaDataMap) throws ParserConfigurationException, TransformerException, SAXException, IOException
	{
		String xmlFilePath=" " ;
		DocumentBuilderFactory dbFactory =
		         DocumentBuilderFactory.newInstance();
		 DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
         Document document = dBuilder.newDocument();
         Element rootElement = document.createElement("managedCall");
         document.appendChild(rootElement);
         Element originator = document.createElement("originator");
         rootElement.appendChild(originator);
         Element hostname = document.createElement("hostName");
         hostname.appendChild(document.createTextNode("hostname"));
         originator.appendChild(hostname);
         Element userID = document.createElement("userID");
         userID.appendChild(document.createTextNode("userID"));
         originator.appendChild(userID);
         Element transferSet = document.createElement("TransferSet");
         Element item = document.createElement("Item");
         transferSet.appendChild(item);
         Element Source = document.createElement("Source");
         item.appendChild(Source);
         Element sourceFile = document.createElement("File");
         rootElement.appendChild(transferSet);
         
         Element triggerPattern = document.createElement("Triggerpattern");
         
        // Element filePattern = document.createElement("FilePattern");
         
         
         
         Element destination = document.createElement("Destination");
         item.appendChild(destination);
         Element destinationFile = document.createElement("File");
         
         Element sourceFilePattern = document.createElement("FilePattern");
         
         
         Element triggerCondition = document.createElement("TriggerCondition");
        
         
         Element poll = document.createElement("Poll");
         transferSet.appendChild(poll);
         Element interval = document.createElement("Interval");
         
         Element units = document.createElement("Units");
         
         
         
         Element Job = document.createElement("Job");
         rootElement.appendChild(Job);
         Element JobName = document.createElement("JobName");
         Attr attr = document.createAttribute("id");
         Attr sattr=document.createAttribute("Disposition");
		for(Map.Entry metaData:metaDataMap.entrySet())
		{
			String key=(String) metaData.getKey();
			String value=(String)metaData.getValue();
						if (metaData.getKey().toString()=="destinationDirectory")
				         {
				    	 destinationFile.appendChild(document.createTextNode(metaData.getValue().toString()));
				         destination.appendChild(destinationFile);
				         }
				     
				     if (metaData.getKey().toString()=="sourceDirectory")
				         {
				    	 sourceFile.appendChild(document.createTextNode(metaData.getValue().toString()));
				         Source.appendChild(sourceFile);
				         }
				     if(metaData.getKey().toString()=="triggerPattern"){
				    	 triggerPattern.appendChild(document.createTextNode(metaData.getValue().toString()));
				         Source.appendChild(triggerPattern);
				         }
				     if(metaData.getKey().toString()=="sourcefilePattern"){
				    	 sourceFilePattern.appendChild(document.createTextNode(metaData.getValue().toString()));
				         Source.appendChild(sourceFilePattern);
				         }
				     if (metaData.getKey().toString()=="destinationFile"){
				         destinationFile.appendChild(document.createTextNode(metaData.getValue().toString()));
				         destination.appendChild(destinationFile);
				         }
//				     if(metaData.getKey().toString()=="filePatternd"){
//				    	 filePatternd.appendChild(document.createTextNode("Fpattern"));
//				         destination.appendChild(filePatternd);
//				         }
				     if(metaData.getKey().toString()=="triggerDestination"){
				    	 triggerCondition.appendChild(document.createTextNode(metaData.getValue().toString()));
				         destination.appendChild(triggerCondition);
				         }
				     if(metaData.getKey().toString()=="pollInterval"){
				    	 interval.appendChild(document.createTextNode(metaData.getValue().toString()));
				         poll.appendChild(interval);
				         }
				     
				     if (metaData.getKey().toString()=="pollUnits"){
				    	 units.appendChild(document.createTextNode(metaData.getValue().toString()));
				         poll.appendChild(units);
				         }
				     if(metaData.getKey().toString()=="jobName"){
				    	 JobName.appendChild(document.createTextNode(metaData.getValue().toString()));
				         Job.appendChild(JobName); 
				     }
					 if(metaData.getKey().toString()=="xmlFilePath"){
						 xmlFilePath= metaData.getValue().toString();
						   //System.out.println(xmlFilePath);
							//MonitorXMLReader.FileMonitor(xmlFilePath);
			        }
					 
					 if (metaData.getKey().toString() == "destinationExists") {

							attr.setValue(metaData.getValue().toString());
							destination.setAttributeNode(attr);
						}
						if (metaData.getKey().toString() != "destinationExists") {
							attr.setValue("Error");
							destination.setAttributeNode(attr);
						}
						if (metaData.getKey().toString() == "sourceDisposition") {

							sattr.setValue(metaData.getValue().toString());
							Source.setAttributeNode(sattr);
						}
						if (metaData.getKey().toString() != "sourceDisposition") {
							sattr.setValue("leave");
							Source.setAttributeNode(sattr);
						}
					 

		 
		}TransformerFactory transformerFactory = TransformerFactory.newInstance();
	        
	       Transformer transformer = transformerFactory.newTransformer();
	        
	        DOMSource domSource = new DOMSource(document);
	         
	         StreamResult streamResult = new StreamResult(new File(xmlFilePath));
	         
	         transformer.transform(domSource, streamResult);
	         System.out.println("created successfully");
	 		
	         
	         
	}
	public static void main(String[] args) throws Exception {
		boolean monitorOverride = false;
		String destinationDirectory = null,sourceDirectory = null,destiationFile=null,sourcefilePattern=null;
		String param, pollInterval, pollUnits,jobName,triggerDestination,triggerPattern,xmlFilePath,monitorName,destinationExists,sourceDisposition;
		Map<String,String> metaDataMap=new HashMap<String,String>();
		for(int i=0;i<args.length;i++)
		{
			param = args[i];
			switch(param)
			{
			case "-dd":
				destinationDirectory = args[i+1];
				sourceDirectory = args[i+2];
				metaDataMap.put("destinationDirectory",destinationDirectory);
				metaDataMap.put("sourceDirectory", sourceDirectory);
				break;
			case "-df":
				destiationFile = args[i+1];
				sourceDirectory = args[i+2];
				metaDataMap.put("destiationFile", destiationFile);
				metaDataMap.put("sourceDirectory", sourceDirectory);
				break;
			case "-tr":
				triggerPattern=args[i+1];
				metaDataMap.put("triggerPattern", triggerPattern);
				break;
			case "-trd":
				triggerDestination=args[i+1];
				metaDataMap.put("triggerDestination", triggerDestination);
				break;
			case "-pi":
				pollInterval=args[i+1];
				metaDataMap.put("pollInterval", pollInterval);
				break;
			case "-pu":
				pollUnits=args[i+1];
				metaDataMap.put("pollUnits", pollUnits);
				break;
			case "-jn":
			    jobName=args[i+1];
			    metaDataMap.put("jobName", jobName);
				break;
			case "-gt":
			     xmlFilePath=args[i+1];
				 metaDataMap.put("xmlFilePath",xmlFilePath);
				 break;
			case "-sfp":
				sourcefilePattern=args[i+1];
				 metaDataMap.put("sourcefilePattern",sourcefilePattern);
				 break;
			
			case "-de":
				destinationExists = args[i + 1];
				if ("overwrite".equalsIgnoreCase(destinationExists)
						|| "error".equalsIgnoreCase(destinationExists)) {
					metaDataMap.put("destinationExists", destinationExists);
				} else {
					throw new InvalidAttributesException();
				}
				break;
			case "-mn":
				monitorName=args[i+1];
				metaDataMap.put("monitorName",monitorName);
				break;
			case "-sd":
				sourceDisposition=args[i+1];
				if ("delete".equalsIgnoreCase(sourceDisposition)
						|| "leave".equalsIgnoreCase(sourceDisposition)) {
					metaDataMap.put("sourceDisposition", sourceDisposition);
				} else {
					throw new InvalidAttributesException();
				}
				break;
			case "-f":
				monitorOverride = true;
		}

		}
		access(metaDataMap);
		
		Session session = DBOperations.connectCassandra();
		if(DBOperations.DBMonitorCheck(session,metaDataMap.get("monitorName"))==null || monitorOverride == true) {
			DBOperations.starting(session, metaDataMap.get("monitorName"));	
		}
		else {
			throw new Exception ("Monitor "+metaDataMap.get("monitorName") +"already exists specify -f parameter to override the previous one");
		}
		
		MyTimerTask.timerAccess(metaDataMap);
		
	}
	}

