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
		String monitorNames [] = request.getParameter("monitorName").split(",");
		String monitorName = monitorNames[1];
		System.out.println(" monitor name is " + monitorName);
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		if(request.getParameter("Delete Button") != null) {
			PrintWriter out = response.getWriter();
			response.setContentType("text/html");
			out.println("<script type=\"text/javascript\">");
			out.println("alert('Do U WANT TO DELETE');");
			if (monitorName != null) {
				System.out.println("in monitor");
				cassandraInteracter.deletingMonitorThread(
						cassandraInteracter.connectCassandra(), monitorName);
			}
			out.println(
					"window.open('http://localhost:8080/OFTE_UI/Open_OFTE_MainHomeData_Pages.html','_self')");
			out.println("</script>");
		}else if(request.getParameter("Pause Button") != null) {
			PrintWriter out = response.getWriter();
			response.setContentType("text/html");
			out.println("<script type=\"text/javascript\">");
			out.println("alert('Monitor has been paused');");
			if (monitorName != null) {
				System.out.println("in monitor");
				cassandraInteracter.pauseMonitorThread(
						cassandraInteracter.connectCassandra(), monitorName);
			}
			out.println(
					"window.open('http://localhost:8080/OFTE_UI/Open_OFTE_MainHomeData_Pages.html','_self')");
			out.println("</script>");
		}else if(request.getParameter("Resume Button") != null) {
			PrintWriter out = response.getWriter();
			response.setContentType("text/html");
			out.println("<script type=\"text/javascript\">");
			out.println("alert('Monitor has been resumed successfully');");
			if (monitorName != null) {
				System.out.println("in monitor");
				cassandraInteracter.started(
						cassandraInteracter.connectCassandra(), monitorName);
			}
			out.println(
					"window.open('http://localhost:8080/OFTE_UI/Open_OFTE_MainHomeData_Pages.html','_self')");
			out.println("</script>");
		}
	}
}
