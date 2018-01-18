
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
 * Servlet implementation class ScheduledTransfer
 */
@SuppressWarnings("serial")
@WebServlet("/ScheduledTransferRemoteasSource")
public class ScheduledTransferRemoteasSource extends HttpServlet {
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
		String schedulerName = request.getParameter("schedulername");
		String jobName = request.getParameter("jname");
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
		HashMap<String, String> hashMap = new HashMap<String, String>();
		System.out.println(sourceDirectory);
		hashMap.put("-sn", schedulerName);
		hashMap.put("-jn", jobName);
		// hashMap.put("sd", sourceDirectory);
		hashMap.put("-sftp-s", sourceDirectory);
		hashMap.put("-tr", sourceTriggerPattern);
		hashMap.put("-sfp", sourceFilePattern);
		hashMap.put("-dd", destinationDirectory);
		hashMap.put("-trd", destinationTriggerPattern);
		hashMap.put("-hi", hostIp);
		hashMap.put("-un", userName);
		hashMap.put("-pw", password);
		hashMap.put("-po", port);
		hashMap.put("-pu", pollUnits);
		hashMap.put("-pi", pollInterval);
		hashMap.put("-dfp", destinationFilePattern);
		System.out.println(hashMap);
		System.out.println(hashMap.size());
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
				"window.open('http://localhost:8080/OFTE_UI/Open_OFTE_Scheduled_Transfers_PageData.html','_self')");
		out.println("</script>");
	}
}