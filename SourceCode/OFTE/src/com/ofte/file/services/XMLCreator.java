package com.ofte.file.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
/**
 * 
 * Class Functionality:
 *                    	The main functionality of this class is Creation of XMLFile in a particular format depending upon the user command
 * 
 * Methods:
 * 			public void access(Map<String, String> metaDataMap) throws SAXException, IOException
 *
 */

public class XMLCreator {
	//Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	//Creating Logger object for XMLCreator class
	Logger logger = Logger.getLogger(XMLCreator.class.getName());
	//Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	 
	@SuppressWarnings("rawtypes")
	/**
	 * This method is uesd to create an XML file based on the given structure
	 * @param metaDataMap
	 * @throws SAXException
	 * @throws IOException
	 */
	public void access(Map<String, String> metaDataMap) throws SAXException, IOException {
		try {
			//Declaration of parameter xmlFilePath
			String xmlFilePath = " ";
			//Creating an object for DocumentBuilderFactory class
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			//Creating an object for DocumentBuilder class
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			//Creating an object for Document class
			Document document = dBuilder.newDocument();
			//Creation of Element rootElement
			Element rootElement = document.createElement("managedCall");
			//Appending rootElement to document
			document.appendChild(rootElement);
			//Creation of Element originator
			Element originator = document.createElement("originator");
			//Appending originator to rootElement
			rootElement.appendChild(originator);
			//Creation of Element hostname
			Element hostname = document.createElement("hostName");
			//Creation of TextNode and appending it to hostname
			hostname.appendChild(document.createTextNode("hostname"));
			//Appending hostname to originator
			originator.appendChild(hostname);
			//Creation of Element userID
			Element userID = document.createElement("userID");
			//Creation of TextNode and appending it to userID
			userID.appendChild(document.createTextNode("userID"));
			//Appending userID to originator
			originator.appendChild(userID);
			//Creation of Element transferSet
			Element transferSet = document.createElement("TransferSet");
			//Creation of Element item
			Element item = document.createElement("Item");
			//Appending item to transferSet
			transferSet.appendChild(item);
			//Creation of Element Source
			Element Source = document.createElement("Source");
			//Appending item to Source
			item.appendChild(Source);
			//Creation of Element sourceFile
			Element sourceFile = document.createElement("File");
			//Appending transferSet  to rootElement
			rootElement.appendChild(transferSet);
			//Creation of Element triggerPattern
			Element triggerPattern = document.createElement("Triggerpattern");
			//Creation of Element destination
			Element destination = document.createElement("Destination");
			//Appending destination to item
			item.appendChild(destination);
			//Creation of Element destinationFile
			Element destinationFile = document.createElement("File");
			//Creation of Element sourceFilePattern
			Element sourceFilePattern = document.createElement("FilePattern");
			//Creation of Element triggerCondition
			Element triggerCondition = document.createElement("TriggerCondition");
			//Creation of Element poll
			Element poll = document.createElement("Poll");
			//Appending poll to transferSet
			transferSet.appendChild(poll);
			//Creation of Element interval
			Element interval = document.createElement("Interval");
			//Creation of Element units
			Element units = document.createElement("Units");
			//Creation of Element Job
			Element Job = document.createElement("Job");
			//Appending Job to rootElement
			rootElement.appendChild(Job);
			//Creation of Element JobName
			Element JobName = document.createElement("JobName");
			//Creation of Attribute attr
			Attr attr = document.createAttribute("id");
			//Creation of Attribute sattr
			Attr sattr = document.createAttribute("Disposition");
			//
			for (Map.Entry metaData : metaDataMap.entrySet()) {
				//Declaration of parameter key and initialising it by getting key from metaData
				String key = (String) metaData.getKey();
				//Declaration of parameter value and initialising it by getting value from metaData
				String value = (String) metaData.getValue();
				//if loop to check the condition whether the key equals to destinationDirectory
				if (key == "destinationDirectory") {
					destinationFile.appendChild(document.createTextNode(value));
					destination.appendChild(destinationFile);
				}
				//if loop to check the condition whether the key equals to sourceDirectory
				if (key == "sourceDirectory") {
					sourceFile.appendChild(document.createTextNode(value));
					Source.appendChild(sourceFile);
				}
				//if loop to check the condition whether the key equals to triggerPattern
				if (key == "triggerPattern") {
					triggerPattern.appendChild(document.createTextNode(value));
					Source.appendChild(triggerPattern);
				}
				//if loop to check the condition whether the key equals to sourcefilePattern
				if (key == "sourcefilePattern") {
					sourceFilePattern.appendChild(document.createTextNode(value));
					Source.appendChild(sourceFilePattern);
				}
				//if loop to check the condition whether the key equals to destinationFile
				if (key == "destinationFile") {
					destinationFile.appendChild(document.createTextNode(value));
					destination.appendChild(destinationFile);
				}
				//if loop to check the condition whether the key equals to triggerDestination
				if (key == "triggerDestination") {
					triggerCondition.appendChild(document.createTextNode(value));
					destination.appendChild(triggerCondition);
				}
				//if loop to check the condition whether the key equals to pollInterval
				if (key == "pollInterval") {
					interval.appendChild(document.createTextNode(value));
					poll.appendChild(interval);
				}
				//if loop to check the condition whether the key equals to pollUnits
				if (key == "pollUnits") {
					units.appendChild(document.createTextNode(value));
					poll.appendChild(units);
				}
				//if loop to check the condition whether the key equals to jobName
				if (key == "jobName") {
					JobName.appendChild(document.createTextNode(value));
					Job.appendChild(JobName);
				}
				//if loop to check the condition whether the key equals to xmlFilePath
				if (key == "xmlFilePath") {
					xmlFilePath = value;
				}
				//if loop to check the condition whether the key equals to destinationExists
				if (key == "destinationExists") {
					attr.setValue(value);
					destination.setAttributeNode(attr);
				}
				//if loop to check the condition key not equals to destinationExists
				if (key != "destinationExists") {
					attr.setValue("Error");
					destination.setAttributeNode(attr);
				}
				//if loop to check the condition whether the key equals to sourceDisposition
				if (key == "sourceDisposition") {
					sattr.setValue(value);
					Source.setAttributeNode(sattr);
				}
				//if loop to check the condition key not equals to sourceDisposition
				if (key != "sourceDisposition") {
					sattr.setValue("leave");
					Source.setAttributeNode(sattr);
				}
			}
			//Creating an object for TransformerFactory class
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			//Creating an object for Transformer class
			Transformer transformer = transformerFactory.newTransformer();
			//Creating an object for DOMSource class
			DOMSource domSource = new DOMSource(document);
			//Creating an object for StreamResult class
			StreamResult streamResult = new StreamResult(new File(xmlFilePath));
			transformer.transform(domSource, streamResult);
		}
		//catching the exception for ParserConfigurationException
		catch (ParserConfigurationException parserConfigurationException) {
			parserConfigurationException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for ParserConfigurationException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		//catching the exception for TransformerException
		catch (TransformerException transformerException) {
			transformerException.printStackTrace(new PrintWriter(log4jStringWriter));
			//logging the exception for TransformerException
			logger.error(loadProperties.getOFTEProperties().getProperty("LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
	}
}