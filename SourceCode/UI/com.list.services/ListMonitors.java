package com.list.services;

import java.util.List;

import com.ofte.services.CassandraInteracter;

public class ListMonitors {
	public static void main(String[] args) {
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		List list = cassandraInteracter
				.getListMonitors(cassandraInteracter.connectCassandra());
		System.out.println(list);
	}

}
