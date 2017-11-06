<%@ page import="java.io.*,
java.util.*,java.sql.*"%>
<%@ page import="javax.servlet.http.*,javax.servlet.*" %>

<%@ taglib uri="http://java.sun.com/jsp/jstl/sql" prefix="sql"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Insert title here</title>
<style>
#ofte {
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
    border-collapse: collapse;
    width: 100%;
}

#ofte td, #ofte th {
    border: 1px solid #ddd;
    padding: 8px;
}

#ofte tr:nth-child(even){background-color: #f2f2f2;}

#ofte tr:hover {background-color: #ddd;}

#ofte th {
    padding-top: 12px;
    padding-bottom: 12px;
    text-align: left;
    background-color: #4c84af;
    color: white;
}
</style>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">

      // Load Charts and the corechart package.
      google.charts.load('current', {'packages':['corechart']});

      // Draw the pie chart when Charts is loaded.
      google.charts.setOnLoadCallback(drawChart);

      // Callback that draws the pie chart.
      function drawChart() {
    	// init google table
    	  var dataTable = new google.visualization.DataTable({
    	      cols: [
    	        { label: 'Transfer_Status', type: 'string' },
    	        { label: 'Success', type: 'number' }
    	      ]
    	  });

    	  // get html table rows
    	  var ofte = document.getElementById('ofte');
    	  Array.prototype.forEach.call(ofte.rows, function(row) {
    	    // exclude column heading
    	    if (row.rowIndex > 0) {
    	      dataTable.addRow([
    	        { v: (row.cells[6].textContent || row.cells[6].innerHTML).trim() },
    	        { v: 1}
    	      ]);
    	    }
    	  });

    	  var dataSummary = google.visualization.data.group(
    	    dataTable,
    	    [0],
    	    [{'column':1, 'aggregation': google.visualization.data.sum, 'type': 'number'}]
    	  );

    	  var options = {
    	    title: 'Logs Chart'
    	  };

    	  var chart = new google.visualization.PieChart(document.getElementById('piechart'));
    	  chart.draw(dataSummary, options);
    	}

    	    </script>
</head>
<body>
<body>
<h1 style="text-align:center;">OFTE EXPLORER</h1>
 <sql:setDataSource var="con" driver="org.apache.cassandra.cql.jdbc.CassandraDriver"
 url="jdbc:cassandra://127.0.0.1:9160/ofte" /> 
 
 
 <%
 Connection con = null;
 try{
 Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
 con = DriverManager.getConnection("jdbc:cassandra://127.0.0.1:9160/ofte");
 
 Statement stmt = con.createStatement();
 ResultSet rs=stmt.executeQuery("select * from monitor_transfer");
 %>
 <table id="ofte">
 <tr>
 <th>Transfer_Id</th><th>Current_Timestamp</th><th>Job_Name</th><th>Monitor_Name</th><th>Source_File</th><th>Target_File</th><th>Transfer_Status</th>
 
 
 
 
 
 
 </tr>
 
<%while(rs.next()) {
out.println("<tr><td>"+rs.getString(1)+"</td><td>"+rs.getString(2)+"</td><td>"+rs.getString(3)+"</td><td>"+rs.getString(4)+"</td> <td>"+rs.getString(5)+"</td><td>"+rs.getString(6)+"</td><td>"+rs.getString(7)+"</td></tr>"); 
}%>
<%
out.println("</table>");
%>

 <%
 }catch (ClassNotFoundException e){
	 e.printStackTrace();
	 }catch (SQLException e){
	 e.printStackTrace();
	 
	 }finally {
	 if(con != null){
	 try{
	 con.close();
	 }catch (SQLException e){
	 e.printStackTrace();
	 }
	 con = null;
	 }
	 }
 %>
 <div id="piechart" style="width: 900px; height: 500px;"></div>
</body>
</html>