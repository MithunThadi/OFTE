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
<title>TransferTableStatus</title>
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
#button {
    width: 15em;
    border: 2px solid black;
    background: #4c84af;
    border-radius: 5px;
}

#button1 {
	float: right;
    width: 10em;
    border: 2px solid black;
    background: #4c84af;
    border-radius: 5px;
}
a {
    display: block;
    width: 100%;
    line-height: 2em;
    text-align: center;
    color:white;
    text-decoration: none;
    border-radius: 5px;
}
a:hover {
    color: black;
    background: #eff;
}

#ofte1 {
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
    border-collapse: collapse;
    width: 100%;
}

#ofte1 td, #ofte th {
    border: 1px solid #ddd;
    padding: 8px;
}

#ofte1 tr:nth-child(even){background-color: #f2f2f2;}

#ofte1 tr:hover {background-color: #ddd;}

#ofte th {
    padding-top: 12px;
    padding-bottom: 12px;
    text-align: left;
    background-color: #4c84af;
    color: white;
}
#button {
    width: 15em;
    border: 2px solid black;
    background: #4c84af;
    border-radius: 5px;
}

#button1 {
	float: right;
    width: 10em;
    border: 2px solid black;
    background: #4c84af;
    border-radius: 5px;
}
a {
    display: block;
    width: 100%;
    line-height: 2em;
    text-align: center;
    color:white;
    text-decoration: none;
    border-radius: 5px;
}
a:hover {
    color: black;
    background: #eff;
}
</style>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
	  function replay(){
		  confirmBox.find(".message").text(msg);
		    confirmBox.find(".yes,.no").unbind().click(function()
		    {
		        confirmBox.hide();
		    });
		    confirmBox.find(".yes").click(yesFn);
		    confirmBox.find(".no").click(noFn);
		    confirmBox.show();
	  }
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
    	        { v: (row.cells[5].textContent || row.cells[5].innerHTML).trim() },
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
    			  title: 'Transfer Logs Chart',
    			  /* slices: {0: { color: 'yellow' },success: { color: 'transparent' }}, */
    			colors: ['#66AA00', '#3366CC', '#FF9900'],
    			  is3D: true
    			};

    	  var chart = new google.visualization.PieChart(document.getElementById('piechart'));
    	  
    	  chart.draw(dataSummary, options);
    	}

    	    </script>
</head>
<body>
<body>
<table id="ofte1">
  <tr>
    <td>
      <h1 style="text-align:center;">OFTE EXPLORER</h1>
      <div id="button1"><a href="http://localhost:8080/OFTE_UI/Open_OFTE_MainHomeData_Pages.html">GO TO HOME PAGE</a></div>
    </td>
    </tr>
    </table>
 <sql:setDataSource var="con" driver="org.apache.cassandra.cql.jdbc.CassandraDriver"
 url="jdbc:cassandra://127.0.0.1:9160/ofte" /> 
 
 
 <%
 Connection con = null;
 try{
	 HashMap<String, String> replayMap = new HashMap<String, String>();
 Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
 con = DriverManager.getConnection("jdbc:cassandra://127.0.0.1:9160/ofte");
 
 Statement stmt = con.createStatement();
 ResultSet rs=stmt.executeQuery("select * from monitor_transfer");
 %>
 
 <table id="ofte">
 <tr>
 <th>Transfer_Id</th>
 <th>Current_Timestamp</th>
 <th>Job_Name</th>
 <th>Source_File</th>
 <th>Target_File</th>
 <th>Transfer_Status</th>
 <th>OverWrite And Replay<th>
 </tr>
 
<%while(rs.next()) {
	String submit = "submit" , replay = "Replay" ,actionForm = "actionForm";
	String transferId = "transferId", hidden = "hidden" , checkBox = "checkbox" ,overWrite = "overwrite";
	if (("success").equalsIgnoreCase(rs.getString(6))){
		out.println("<tr><td>"+rs.getString(1)+"</td><td>"+rs.getString(2)+"</td><td>"+rs.getString(3)+"</td><td>"+rs.getString(4)+"</td> <td>"+rs.getString(5)+"</td><td>"+rs.getString(6)+"</td></tr>"); 
		//System.out.println(request.getAttribute("transferId"));
		}else if (("failure").equalsIgnoreCase(rs.getString(6))){
			request.setAttribute("transferId", rs.getString(1));
			out.println("<tr><td>"+rs.getString(1)+"</td><td>"+rs.getString(2)+"</td><td>"+rs.getString(3)+"</td><td>"+rs.getString(4)+"</td> <td>"+rs.getString(5)+"</td><td>"+rs.getString(6)+"</td><td><form NAME="+actionForm+" action="+replay+" method='post'><input type="+hidden+" name="+transferId+" value="+rs.getString(1)+"><input  type="+checkBox+" name="+overWrite+"><input type="+submit+" value = "+replay+"></form></td></tr>"); 
			//System.out.println(request.getAttribute("transferId"));
	}else {
		out.println("<tr><td>"+rs.getString(1)+"</td><td>"+rs.getString(2)+"</td><td>"+rs.getString(3)+"</td><td>"+rs.getString(4)+"</td> <td>"+rs.getString(5)+"</td><td>"+rs.getString(6)+"</td></tr>"); 
	}
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