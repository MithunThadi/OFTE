<%@ page import="java.io.*,
java.util.*,java.sql.*"%>
<%@ page import="javax.servlet.http.*,javax.servlet.*" %>

<%@ taglib uri="http://java.sun.com/jsp/jstl/sql" prefix="sql"%>

<html>
<head>
<title>DeleteScheduler</title>
<style>
body {
font-family:sans-serif;
font-weight:normal;
margin:10px;
color:black;
background-color:#eee;
}
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
    background-color: #4CAF50;
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
fieldset {
    width: 90%;
    padding:20px;
    border-radius:3px;
	border:1px solid #D1D3D4
   
}
div{
clear:both;
margin:0 25px;
}
label
{
background-color:#3177b4;
color:white;
width:200px;
border-radius:3px;
border:1px solid #D1D3D4
padding:20px;
}
label
{
width:200px;
border-radius:3px;
border:1px solid #D1D3D4
padding:20px;
}
form {
	
	background-color:#fefefe;
	margin:2% auto 15% auto;
	border:5px solid #888;
	width:100%;
}
	.labelClass
{
background-color:#3177b4;
color:white;
}
.modal
{
display:none;
position:fixed;
z-index:100;
left:0;
top:0;
width:10%;
height:100%;
overflow:auto;
background-color:white;
padding-top:60px;
}
.modal-content
{
background-color:#fefefe;
margin:2% auto 15% auto;
border:5px solid #888;
width:90%;
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
 //ResultSet rs=stmt.executeQuery("select monitor_name from monitor");
 ResultSet rs1=stmt.executeQuery("select scheduler_name from scheduler");
 
 //ResultSet rs1 = stmt.executeQuery("select * from monitor_metadata where monitor_name ="+rs.getString(1));
 %>
 
 <form name="form" id="form" class="modal-content" method="post" action="DeleteScheduler">
 <center>
 <br><br>
    <label for="SchedulerNames">SchedulerNames:</label> 
        <select name="schedulerName">
        <option selected="" value="">Select</option>
        <%  while(rs1.next()){ %>
            <option><%= rs1.getString(1)%></option>
        <% } %>
        
        </select>
</center>
 

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
 
<input type="submit" value="Delete" />
</form>
</body>
</html>