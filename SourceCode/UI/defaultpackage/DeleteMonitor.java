
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ofte.services.CassandraInteracter;

/**
 * Servlet implementation class XMLCreatorServlet
 */
@SuppressWarnings("serial")
@WebServlet("/DeleteMonitor")
public class DeleteMonitor extends HttpServlet {
	// // private static final long serialVersionUID = 1L;
	// HashMap<String, String> hashMap = new HashMap<String, String>();
	// // com.ofte.services.MetaDataCreations metaDataCreations = new
	// // MetaDataCreations();
	// MetaDataCreations metaDataCreations = new MetaDataCreations();
	// /**
	// * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	// * response)
	// */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// // TODO Auto-generated method stub
		String monitorName = request.getParameter("monitorName");
		System.out.println(" monitor name is " + monitorName);
		// String schedulerName = request.getParameter("SchedulerName");
		// System.out.println(" schedulerName name is " + schedulerName);
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		out.println("<script type=\"text/javascript\">");
		out.println("alert('Do U WANT TO DELETE');");
		// out.println(
		// "window.open('http://localhost:8080/TestingUI/Open_OFTE_Monitor_XMLCreator_Pages.html','_self')");
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		if (monitorName != null) {
			System.out.println("in monitor");
			cassandraInteracter.deletingMonitorThread(
					cassandraInteracter.connectCassandra(), monitorName);
		}
		// if (schedulerName != null) {
		// cassandraInteracter.deletingSchedulerThread(
		// cassandraInteracter.connectCassandra(), schedulerName);
		// }
		out.println(
				"window.open('http://localhost:8080/Testing_UI/Open_OFTE_MainHome_Pages.html','_self')");
		out.println("</script>");

	}
}
