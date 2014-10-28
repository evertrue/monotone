package com.et.monotone;

import redis.clients.jedis.Jedis;

import com.et.monotone.redis.RedisGenerator;
import com.et.monotone.zk.ZKGenerator;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class Driver {

	public static void main(String[] args) {
		RetryPolicy policy = new RetryNTimes(1, 100);
		CuratorFramework zkClient = CuratorFrameworkFactory.newClient("localhost:2181", 3000, 3000, policy);
		zkClient.start();
		IdGenerator zkGen = ZKGenerator.newBuilder(zkClient).setCounterName("mark_test").setMaxIdsToFetch(1000)
				.setRootPath("/id_gen").build();

		runGenerator(zkGen);

		Jedis redisClient = new Jedis("localhost");;

		IdGenerator redisGen = RedisGenerator.newBuilder(redisClient).setCounterName("alex_test").setMaxIdsToFetch(1000).build();

		runGenerator(redisGen);
	}

	public static void runGenerator(IdGenerator gen) {
		System.out.println("Starting");
		long start = System.currentTimeMillis();
		for (int i = 0; i < 50000; i++) {
			
			long id = gen.nextId();
			System.out.println(id);
		}

		System.out.println("Took : " + String.valueOf(System.currentTimeMillis() - start) + " milliseconds");
	}

}
