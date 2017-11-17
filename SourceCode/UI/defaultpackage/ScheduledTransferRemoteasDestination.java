
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ofte.services.MetaDataCreations;

/**
 * Servlet implementation class ScheduledTransferRemoteasDestination
 */
@SuppressWarnings("serial")
@WebServlet("/ScheduledTransferRemoteasDestination")
public class ScheduledTransferRemoteasDestination extends HttpServlet {
	// private static final long serialVersionUID = 1L;
	HashMap<String, String> hashMap = new HashMap<String, String>();
	// com.ofte.services.MetaDataCreations metaDataCreations = new
	// MetaDataCreations();
	MetaDataCreations metaDataCreations = new MetaDataCreations();
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub

		// read form fields
		String schedulername = request.getParameter("schedulername");
		// String jobName = request.getParameter("jname");
		String sourceDirectory = request.getParameter("sd");
		String sourceTriggerPattern = request.getParameter("stp");
		String sourceFilePattern = request.getParameter("sfp");
		String destinationDirectory = request.getParameter("dd");
		String destinationFilePattern = request.getParameter("dfp");
		String destinationTriggerPattern = request.getParameter("dtp");
		String hostIp = request.getParameter("hostip");
		String userName = request.getParameter("username");
		String password = request.getParameter("password");
		String port = request.getParameter("port");
		String pollUnits = request.getParameter("pu");
		String pollInterval = request.getParameter("pi");
		// String XMLFilePath = request.getParameter("xmlfilename");
		HashMap<String, String> hashMap = new HashMap<>();
		hashMap.put("-sn", schedulername);
		// hashMap.put("-jn", jobName);
		hashMap.put("-sd", sourceDirectory);
		hashMap.put("-tr", sourceTriggerPattern);
		hashMap.put("-sfp", sourceFilePattern);
		hashMap.put("-sftp-d", destinationDirectory);
		hashMap.put("-trd", destinationTriggerPattern);
		hashMap.put("-hi", hostIp);
		hashMap.put("-un", userName);
		hashMap.put("-pw", password);
		hashMap.put("-po", port);
		hashMap.put("-pu", pollUnits);
		hashMap.put("-pi", pollInterval);
		hashMap.put("-dfp", destinationFilePattern);
		// hashMap.put("-gt", XMLFilePath);
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
		// String string = "-mn "+monitorName+",-jn "+jobName+",-pi
		// "+pollInterval+",-pu "+pollUnits+",-dd "+destinationDirectory+" "+
		// sourceDirectory+",-tr "+sourceTriggerPattern+",-trd
		// "+destinationTriggerPattern+",-gt "+XMLFilePath+",-sfp
		// "+sourceFilePattern;
		// FileWriter fileWriter = new FileWriter("D:\\UIDetails.txt");
		// fileWriter.write(string);
		// fileWriter.close();
		Runnable r = new Runnable() {
			public void run() {
				// runYourBackgroundTaskHere();
				try {
					metaDataCreations.fetchingUIDetails(hashMap);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		new Thread(r).start();
		// Example example = new Example();
		// hashMap.put("monitorName", monitorName);
		// example.result("-mn "+monitorName+" -jn "+jobName+" -pi
		// "+pollInterval+" -pu "+pollUnits+" -dd "+destinationDirectory+" "+
		// sourceDirectory+" -tr "+sourceTriggerPattern+" -trd
		// "+destinationTriggerPattern+" -gt "+XMLFilePath+" -sfp
		// "+sourceFilePattern);

		// do some processing here...

		// get response writer
		// PrintWriter writer = response.getWriter();

		// build HTML code
		// String htmlRespone = "<html>";
		// htmlRespone += "<h2>Your username is: " + username + "<br/>";
		// htmlRespone += "Your password is: " + password + "</h2>";
		// htmlRespone += "</html>";

		// return response
		// writer.println(htmlRespone);

		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		out.println("<script type=\"text/javascript\">");
		out.println("alert('successfully submited');");
		out.println(
				"window.open('http://localhost:8080/TestingUI/Open_OFTE_Scheduled_Transfers_Page.html','_self')");
		out.println("</script>");

	}
}
