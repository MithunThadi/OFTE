<%@ page import="java.io.*,
java.util.*,java.sql.*"%>
<%@ page import="javax.servlet.http.*,javax.servlet.*" %>

<%@ taglib uri="http://java.sun.com/jsp/jstl/sql" prefix="sql"%>

<html>
<head>
<title>MonitorTransferTable</title>
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
</style>
</head>
<body>
<table id="ofte">
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
 <th>Monitor_Name</th>
 <th>Source_File</th>
 <th>Target_File</th>
 <th>Transfer_Status</th>
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
 		 
</body>
</html>