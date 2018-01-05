package com.ofte.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class UserExitPoints {

	// Creating an object for LoadProperties class
	LoadProperties loadProperties = new LoadProperties();
	// Creating Logger object for FilesProcessorService class
	Logger logger = Logger.getLogger(FilesProcessorService.class.getName());
	// Creating an object for StringWriter class
	StringWriter log4jStringWriter = new StringWriter();
	public int accessExitPoint(String exitPoint,
			Map<String, String> metaDataMap,
			Map<String, String> transferMetaData) {
		int result = 0;
		try {
			String[] mapDataArrays = exitPoint.split("~");
			// Operations operations = new Operations();
			// for loop to put the values into Map object
			for (int j = 0; j < mapDataArrays.length; j++) {
				// String[] mapDataArrays1 = mapDataArrays[j]
				// .split("\\|");
				// String operation = mapDataArrays1[1];
				DocumentBuilderFactory factory = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder builder = factory.newDocumentBuilder();
				File xmlFile = new File(loadProperties.getOFTEProperties()
						.getProperty("USER_EXIT_XML"));
				Document doc = builder.parse(xmlFile);
				Element docElement = doc.getDocumentElement();
				System.out.println(docElement.getNodeName());
				NodeList list = docElement.getElementsByTagName("operation");
				System.out.println(
						list.item(j).getChildNodes().item(0).getNodeValue());
				System.out.println(list.item(j).getAttributes()
						.getNamedItem("Name").getNodeValue());
				// String operationName = list.item(j).getAttributes()
				// .getNamedItem("Name").getNodeValue();
				String class1 = list.item(j).getChildNodes().item(0)
						.getNodeValue();
				Class<?> classType = Class.forName(class1);
				Object obj = classType;
				System.out.println(obj.getClass().getName().toString());
				String[] value = ((mapDataArrays[j].replaceAll("\\|", ","))
						.substring(mapDataArrays[j].indexOf("|") + 1))
								.split(",");
				// System.out.println(value);
				Method[] methods = classType.getDeclaredMethods();
				// for(int
				// m=0;m<classType.getDeclaredMethods().length;m++)
				// {
				for (int i = 0; i < methods.length; i++) {
					System.out.println(methods[i]);
					// }
					System.out.println(methods[i].getName());
					result = (int) methods[i].invoke(obj, value, metaDataMap,
							transferMetaData);
					System.out.println(result);
				}

			}
		}
		// catching the exception for ClassNotFoundException
		catch (ClassNotFoundException classNotFoundException) {
			classNotFoundException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for ClassNotFoundException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for IllegalAccessException
		catch (IllegalAccessException illegalAccessException) {
			illegalAccessException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for IllegalAccessException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for IllegalArgumentException
		catch (IllegalArgumentException illegalArgumentException) {
			illegalArgumentException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for IllegalArgumentException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for InvocationTargetException
		catch (InvocationTargetException invocationTargetException) {
			invocationTargetException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for InvocationTargetException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for ParserConfigurationException
		catch (ParserConfigurationException parserConfigurationException) {
			parserConfigurationException
					.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for ParserConfigurationException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for SAXException
		catch (SAXException SAXException) {
			SAXException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for SAXException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		// catching the exception for IOException
		catch (IOException ioException) {
			ioException.printStackTrace(new PrintWriter(log4jStringWriter));
			// logging the exception for IOException
			logger.error(loadProperties.getOFTEProperties().getProperty(
					"LOGGEREXCEPTION") + log4jStringWriter.toString());
		}
		return result;
	}

}
