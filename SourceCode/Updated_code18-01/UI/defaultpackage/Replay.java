
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ofte.cassandra.services.CassandraInteracter;
import com.ofte.file.services.ProcessFiles;

/**
 * Servlet implementation class Replay
 */
@WebServlet("/Replay")
public class Replay extends HttpServlet {
	private static final long serialVersionUID = 1L;
	CassandraInteracter cassandraInteracter = new CassandraInteracter();
	Map<String, String> hashMap = new HashMap<String, String>();
	ProcessFiles processFiles = new ProcessFiles();
	LinkedList<String> processFileList = new LinkedList<String>();

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		// doGet(request, response);
		
		String transferId = request.getParameter("transferId");
		String statuss = request.getParameter("overwrite");
		System.out.println(statuss);
		String sourceFile = cassandraInteracter.getSourceFile(
				cassandraInteracter.connectCassandra(), transferId);
		String monitorName = cassandraInteracter.getMonitorName(
				cassandraInteracter.connectCassandra(), transferId);
		System.out.println(monitorName);
		String metadata = cassandraInteracter.getMonitorMetaData(
				cassandraInteracter.connectCassandra(), monitorName);
		

		String[] mapDataArrays = metadata.split(",");
		// for loop to put the values into Map object
		for (int j = 0; j < mapDataArrays.length; j++) {
			hashMap.put(
					(mapDataArrays[j].substring(0,
							(mapDataArrays[j].indexOf("=")))).toString(),
					((mapDataArrays[j]
							.substring(mapDataArrays[j].indexOf("=") + 1)))
									.toString());

		}
		hashMap.put("transferId", transferId);
		if (statuss.equalsIgnoreCase("on")) {
			
			hashMap.replace("destinationExists", "overWrite");
			System.out.println(hashMap.get("destinationExists"));
		}
		// hashMap.put("sourcefilePattern", "*.txt");
		// hashMap.put("triggerPattern", "*.txt");
		// hashMap.put("destinationFilePattern", "*.txt");
		// hashMap.put("sourceDirectory",
		// (hashMap.get("source_file")).substring(0,
		// (hashMap.get("source_file")).lastIndexOf("/")));
		// hashMap.put("destinationDirectory",
		// (hashMap.get("target_file")).substring(0,
		// (hashMap.get("target_file")).lastIndexOf("/")));
		// hashMap.put("destinationDirectory", "D:\\open\\Destination");
		String fileName = null;
		if(System.getProperty("os.name").contains("Linux")) {
		 fileName = ((sourceFile)
				.substring(sourceFile.lastIndexOf("/") + 1));
		}else if(System.getProperty("os.name").contains("Windows")) {
			 fileName = ((sourceFile)
						.substring(sourceFile.lastIndexOf("\\") + 1));
		}
		// String value = hashMap.get("source_file");
		// value.substri
		System.out.println(hashMap);
		System.out.println(fileName);
		// System.out.println(hashMap.get("sourceDirectory"));
		processFileList.clear();
		processFileList.add(fileName);
		try {
			processFiles.processFileList(processFileList, hashMap);
		} catch (InterruptedException | NoSuchMethodException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		out.println("<script type=\"text/javascript\">");
		out.println("alert('The file transfer is replayed successfully');");
		out.println(
				"window.open('http://localhost:8080/OFTE_UI/Open_OFTE_Monitor_TransferTable_StatusPages.jsp','_self')");
		out.println("</script>");
	}
}
