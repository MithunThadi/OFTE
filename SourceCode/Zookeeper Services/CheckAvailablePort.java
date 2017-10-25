package zookeeper.services;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
//import java.net.UnknownHostException;

public class CheckAvailablePort {
	
	static boolean availablePort(String host, int port) throws IOException {
		  // Assume port is available.
		  boolean result = true;

		  try {
		    (new Socket(host, port)).isClosed();

		    // Successful connection means the port is taken.
		    result = false;
		  }
		  catch(SocketException e) {
		    // Could not connect.
			  System.out.println(e.getMessage());
		  }

		  return result;
		}
	public static void main (String args[]) throws IOException
	{
		System.out.println(availablePort("localhost", 9362));
	
	}
	
}

