package com.list.services;

import java.util.List;

import com.ofte.services.CassandraInteracter;

public class ListScheduler {
	public static void main(String[] args) {
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		List l = cassandraInteracter
				.getListSchedulers(cassandraInteracter.connectCassandra());
		System.out.println(l);
	}

}
