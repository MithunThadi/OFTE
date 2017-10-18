package commm;

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

public class XMLCreator {
	 Logger logger = Logger.getLogger(XMLCreator.class.getName());
	  String loggerException="Caught exception; decorating with appropriate status template : ";

	public void access(Map<String, String> metaDataMap) throws SAXException, IOException {
		try {
			String xmlFilePath = " ";
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
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
			Attr sattr = document.createAttribute("Disposition");
			for (Map.Entry metaData : metaDataMap.entrySet()) {
				String key = (String) metaData.getKey();
				String value = (String) metaData.getValue();
				if (key == "destinationDirectory") {
					destinationFile.appendChild(document.createTextNode(value));
					destination.appendChild(destinationFile);
				}

				if (key == "sourceDirectory") {
					sourceFile.appendChild(document.createTextNode(value));
					Source.appendChild(sourceFile);
				}
				if (key == "triggerPattern") {
					triggerPattern.appendChild(document.createTextNode(value));
					Source.appendChild(triggerPattern);
				}
				if (key == "sourcefilePattern") {
					sourceFilePattern.appendChild(document.createTextNode(value));
					Source.appendChild(sourceFilePattern);
				}
				if (key == "destinationFile") {
					destinationFile.appendChild(document.createTextNode(value));
					destination.appendChild(destinationFile);
				}
				if (key == "triggerDestination") {
					triggerCondition.appendChild(document.createTextNode(value));
					destination.appendChild(triggerCondition);
				}
				if (key == "pollInterval") {
					interval.appendChild(document.createTextNode(value));
					poll.appendChild(interval);
				}

				if (key == "pollUnits") {
					units.appendChild(document.createTextNode(value));
					poll.appendChild(units);
				}
				if (key == "jobName") {
					JobName.appendChild(document.createTextNode(value));
					Job.appendChild(JobName);
				}
				if (key == "xmlFilePath") {
					xmlFilePath = value;

				}
				if (key == "destinationExists") {
					attr.setValue(value);
					destination.setAttributeNode(attr);
				}
				if (key != "destinationExists") {
					attr.setValue("Error");
					destination.setAttributeNode(attr);
				}
				if (key == "sourceDisposition") {
					sattr.setValue(value);
					Source.setAttributeNode(sattr);
				}
				if (key != "sourceDisposition") {
					sattr.setValue("leave");
					Source.setAttributeNode(sattr);
				}
			}
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource domSource = new DOMSource(document);
			StreamResult streamResult = new StreamResult(new File(xmlFilePath));
			transformer.transform(domSource, streamResult);
		} catch (ParserConfigurationException parserConfigurationException) {
			StringWriter stack = new StringWriter();
			parserConfigurationException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		} catch (TransformerException transformerException) {
			StringWriter stack = new StringWriter();
			transformerException.printStackTrace(new PrintWriter(stack));
			logger.error(loggerException + stack.toString());
		}
	}
}
