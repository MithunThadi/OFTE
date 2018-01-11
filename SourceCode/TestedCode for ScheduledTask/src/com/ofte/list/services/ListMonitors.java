package com.ofte.list.services;

import java.util.List;

import com.ofte.cassandra.services.CassandraInteracter;

public class ListMonitors {
	public static void main(String[] args) {
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		List<?> list = cassandraInteracter
				.getListMonitors(cassandraInteracter.connectCassandra());

		if (list != null) {
			for (int i = 0; i < list.size(); i++) {
				System.out.println(list.get(i));
			}
		} else {
			System.out.println("No Monitors to display");
		}

	}

}
