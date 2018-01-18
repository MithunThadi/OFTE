
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ofte.file.services.MetaDataCreator;
//import com.ofte.services.MetaDataCreations;

/**
 * Servlet implementation class XMLCreatorServlet
 */
@SuppressWarnings("serial")
@WebServlet("/XMLCreatorServlet")
public class XMLCreatorServlet extends HttpServlet {
	// private static final long serialVersionUID = 1L;
	HashMap<String, String> hashMap = new HashMap<String, String>();
	// com.ofte.services.MetaDataCreations metaDataCreations = new
	// MetaDataCreations();
//	MetaDataCreations metaDataCreations = new MetaDataCreations();
	MetaDataCreator metaDataCreator = new MetaDataCreator();
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub

		// read form fields
		String monitorName = request.getParameter("mname");
		String jobName = request.getParameter("jname");
		String sourceDirectory = request.getParameter("sd");
		String sourceTriggerPattern = request.getParameter("stp");
		String sourceFilePattern = request.getParameter("sfp");
		String destinationDirectory = request.getParameter("dd");
		String destinationFilePattern = request.getParameter("dfp");
		String destinationTriggerPattern = request.getParameter("dtp");
		String pollUnits = request.getParameter("pu");
		String pollInterval = request.getParameter("pi");
		String XMLFilePath = request.getParameter("xmlfilename");
		String sourceDisposition = request.getParameter("sourceDisposition");
		String destinationExists = request.getParameter("de");
		String monitorOverWrite = request.getParameter("monitorOverwrite");
		HashMap<String, String> hashMap = new HashMap<>();
		hashMap.put("-mn", monitorName);
		hashMap.put("-jn", jobName);
		hashMap.put("sourceDirectory", sourceDirectory);
		hashMap.put("-tr", sourceTriggerPattern);
		hashMap.put("-sfp", sourceFilePattern);
		hashMap.put("-dd", destinationDirectory);
		hashMap.put("-trd", destinationTriggerPattern);
		hashMap.put("-pu", pollUnits);
		hashMap.put("-pi", pollInterval);
		hashMap.put("-gt", XMLFilePath);
		hashMap.put("-dfp", destinationFilePattern);
		hashMap.put("-sd", sourceDisposition);
		hashMap.put("-de", destinationExists);
		hashMap.put("-f", monitorOverWrite);
		if (!request.getParameter("presrc").isEmpty()) {
		String preSource = request.getParameter("presrc");
		hashMap.put("-preSrc", preSource);
		}
		if (!request.getParameter("predest").isEmpty()) {
		String preDestination = request.getParameter("predest");
		hashMap.put("-preDst", preDestination);
		}
		if (!request.getParameter("postdest").isEmpty()) {
		String postDestination = request.getParameter("postdest");
		hashMap.put("-postDst", postDestination);
		}
		if (!request.getParameter("postsrc").isEmpty()) {
		String postSource = request.getParameter("postsrc");
		hashMap.put("-postSrc", postSource);
		}
		// hashMap.put("-sd", destinationFilePattern);
		// System.out.println(hashMap);
		// System.out.println("username: " + username);
		// System.out.println("password: " + password);
		// String str[] = {"-mn "+monitorName,"-jn "+jobName,"-sd
		// "+sourceDirectory,"-tr "+sourceTriggerPattern,"-sfp
		// "+sourceFilePattern,"-dd
		// "+destinationDirectory,destinationFilePattern,"-trd
		// "+destinationTriggerPattern,"-pu "+pollUnits,"-pi "+pollInterval,"-gt
		// "+XMLFilePath};
		// for(int i=0;i<str.length;i++) {
		// System.out.println(str[i]);
		// }
		try {
			// metaDataCreations.fetchingUIDetails(hashMap);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// String string = "-mn " + monitorName + ",-jn " + jobName + ",-pi "
		// + pollInterval + ",-pu " + pollUnits + ",-dd "
		// + destinationDirectory + " " + sourceDirectory + ",-tr "
		// + sourceTriggerPattern + ",-trd " + destinationTriggerPattern
		// + ",-gt " + XMLFilePath + ",-sfp " + sourceFilePattern;
		// FileWriter fileWriter = new FileWriter("D:\\UIDetails.txt");
		// fileWriter.write(string);
		// fileWriter.close();
		Runnable r = new Runnable() {
			public void run() {
				// runYourBackgroundTaskHere();
				try {
					metaDataCreator.fetchingUIDetails(hashMap);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		new Thread(r).start();

		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		out.println("<script type=\"text/javascript\">");
		out.println("alert('successfully submited');");
		out.println(
				"window.open('http://localhost:8080/OFTE_UI/Open_OFTE_Monitor_XMLCreator_PagesData.html','_self')");
		out.println("</script>");

	}
}
