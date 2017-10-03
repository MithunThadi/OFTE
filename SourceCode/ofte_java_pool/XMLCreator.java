package ofte;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class XMLCreator {
	public static void access(Map<String, String> metaDataMap)
			throws ParserConfigurationException, TransformerException, SAXException, IOException {
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
			if (metaData.getKey().toString() == "destinationDirectory") {
				destinationFile.appendChild(document.createTextNode(metaData.getValue().toString()));
				destination.appendChild(destinationFile);
			}

			if (metaData.getKey().toString() == "sourceDirectory") {
				sourceFile.appendChild(document.createTextNode(metaData.getValue().toString()));
				Source.appendChild(sourceFile);
			}
			if (metaData.getKey().toString() == "triggerPattern") {
				triggerPattern.appendChild(document.createTextNode(metaData.getValue().toString()));
				Source.appendChild(triggerPattern);
			}
			if (metaData.getKey().toString() == "sourcefilePattern") {
				sourceFilePattern.appendChild(document.createTextNode(metaData.getValue().toString()));
				Source.appendChild(sourceFilePattern);
			}
			if (metaData.getKey().toString() == "destinationFile") {
				destinationFile.appendChild(document.createTextNode(metaData.getValue().toString()));
				destination.appendChild(destinationFile);
			}
			if (metaData.getKey().toString() == "triggerDestination") {
				triggerCondition.appendChild(document.createTextNode(metaData.getValue().toString()));
				destination.appendChild(triggerCondition);
			}
			if (metaData.getKey().toString() == "pollInterval") {
				interval.appendChild(document.createTextNode(metaData.getValue().toString()));
				poll.appendChild(interval);
			}

			if (metaData.getKey().toString() == "pollUnits") {
				units.appendChild(document.createTextNode(metaData.getValue().toString()));
				poll.appendChild(units);
			}
			if (metaData.getKey().toString() == "jobName") {
				JobName.appendChild(document.createTextNode(metaData.getValue().toString()));
				Job.appendChild(JobName);
			}
			if (metaData.getKey().toString() == "xmlFilePath") {
				xmlFilePath = metaData.getValue().toString();

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
		}
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource domSource = new DOMSource(document);
		StreamResult streamResult = new StreamResult(new File(xmlFilePath));
		transformer.transform(domSource, streamResult);
	}
}
