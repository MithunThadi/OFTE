package com.delete.services;

import com.ofte.services.CassandraInteracter;

public class DeleteMonitor {
	public static void main(String[] args) {
		String monitorName = args[0];
		if (monitorName != null) {
			CassandraInteracter cassandraInteracter = new CassandraInteracter();
			cassandraInteracter.deletingMonitorThread(
					cassandraInteracter.connectCassandra(), monitorName);
			System.out.println(" updated  successfully as deleted");
		} else {
			try {
				throw new Exception("monitor name should not be empty");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
