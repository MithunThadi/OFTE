package com.ofte.list.services;

import com.ofte.cassandra.services.CassandraInteracter;

public class SchedulerMetaData {
	public static void main(String[] args) {
		String schedulername = args[0];
		System.out.println(schedulername);
		if(args[1]!=null) {
		if (args[1].equalsIgnoreCase("-v")) {
			CassandraInteracter cassandraInteracter = new CassandraInteracter();
			String schedulerMetaData = cassandraInteracter.getSchedulerMetaData(
					cassandraInteracter.connectCassandra(), schedulername);
			System.out.println(schedulerMetaData);
		}
	}
	}

}
