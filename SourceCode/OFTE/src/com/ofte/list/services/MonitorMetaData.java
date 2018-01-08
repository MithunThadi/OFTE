package com.ofte.list.services;

import com.ofte.cassandra.services.CassandraInteracter;

public class MonitorMetaData {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String monitorName = args[0];
		System.out.println(monitorName);
		if (args[1] != null) {
			if (args[1].equalsIgnoreCase("-v")) {
				CassandraInteracter cassandraInteracter = new CassandraInteracter();
				String monitorMetadata = cassandraInteracter.getMonitorMetaData(
						cassandraInteracter.connectCassandra(), monitorName);

				System.out.println(monitorMetadata);
			}
		}
	}

}
