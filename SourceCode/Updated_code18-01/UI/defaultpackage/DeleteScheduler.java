
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ofte.cassandra.services.CassandraInteracter;

/**
 * Servlet implementation class XMLCreatorServlet
 */
@SuppressWarnings("serial")
@WebServlet("/DeleteScheduler")
public class DeleteScheduler extends HttpServlet {
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
		String schedulerName = request.getParameter("schedulerName");
		System.out.println(" schedulerName name is " + schedulerName);
		// String schedulerName = request.getParameter("SchedulerName");
		// System.out.println(" schedulerName name is " + schedulerName);
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		out.println("<script type=\"text/javascript\">");
		out.println("alert('Do U WANT TO DELETE');");
		// out.println(
		// "window.open('http://localhost:8080/TestingUI/Open_OFTE_Monitor_XMLCreator_Pages.html','_self')");
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		if (schedulerName != null) {
			// System.out.println("in monitor");
			cassandraInteracter.deletingSchedulerThread(
					cassandraInteracter.connectCassandra(), schedulerName);
		}
		// if (schedulerName != null) {
		// cassandraInteracter.deletingSchedulerThread(
		// cassandraInteracter.connectCassandra(), schedulerName);
		// }
		out.println(
				"window.open('http://localhost:8080/OFTE_UI/Open_OFTE_MainHomeData_Pages.html','_self')");
		out.println("</script>");

	}
}
