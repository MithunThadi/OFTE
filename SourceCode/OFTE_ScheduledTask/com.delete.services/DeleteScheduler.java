package com.delete.services;

import com.ofte.services.CassandraInteracter;

public class DeleteScheduler {
	public static void main(String[] args) {
		String schedulerName = args[0];
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		if (schedulerName != null) {
			cassandraInteracter.deletingSchedulerThread(
					cassandraInteracter.connectCassandra(), schedulerName);
			System.out.println(schedulerName + " is successfully deleted");
		} else {
			try {
				throw new Exception("scheduler name should not be empty");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
