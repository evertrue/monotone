package com.et.monotone;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class Driver {

	public static void main(String[] args) {
		RetryPolicy policy = new RetryNTimes(1, 100);
		CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", 3000, 3000, policy);
		client.start();
		ZKGenerator gen = ZKGenerator.newBuilder(client).setCounterName("mark_test").setMaxIdsToFetch(50)
				.setRootPath("/id_gen").build();

		System.out.println("Starting");
		long start = System.currentTimeMillis();
		for (int i = 0; i < 5000; i++) {
			
			long id = gen.nextId();
			System.out.println(id);
		}

		System.out.println("Took : " + String.valueOf(System.currentTimeMillis() - start));
	}

}
