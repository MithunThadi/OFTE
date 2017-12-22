package com.list.services;

import java.util.List;

import com.ofte.services.CassandraInteracter;

public class ListScheduler {
	public static void main(String[] args) {
		CassandraInteracter cassandraInteracter = new CassandraInteracter();
		List<?> list = cassandraInteracter
				.getListSchedulers(cassandraInteracter.connectCassandra());
		if (list != null) {
			for (int i = 0; i < list.size(); i++) {
				System.out.println(list.get(i));
			}
		} else {
			System.out.println("No Schedulers to display");
		}

	}

}
