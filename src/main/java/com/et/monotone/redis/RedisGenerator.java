package com.et.monotone.redis;

import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.Jedis;

import com.et.monotone.IdGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.Range;

public class RedisGenerator implements IdGenerator {
	private final static int MAX_ATTEMPTS = 5;
	private Range<Long> idRange;
	private final AtomicLong idCounter;
	private int maxIdsToFetch;
	private Jedis client;
	private String counterName;

	private RedisGenerator(Jedis client, String counterName, int maxIdsToFetch) {
		this.idCounter = new AtomicLong(0);
		this.idRange = Range.closedOpen(0l, 0l);
		this.maxIdsToFetch = maxIdsToFetch;
		this.client = client;
		this.counterName = counterName;

		nextId();
	}

	@Override
	public long nextId() {
		if (idCounter.get() == 0) {
			client.set(counterName, "0");
			refreshAndSetNewRange();
		}

		long nextId = idCounter.getAndIncrement();

		while (!idRange.contains(nextId)) {
			refreshAndSetNewRange();
			nextId = idCounter.getAndIncrement();
		}

		return nextId;
	}

	public static RedisGenerator.Builder newBuilder(Jedis client) {
		return new Builder(client);
	}

	private void refreshAndSetNewRange() {
		synchronized (idCounter) {
			if (idRange.contains(idCounter.get())) {
				return;
			}

			try {
				for (int i = 0; i < MAX_ATTEMPTS; i++) {
					// Get latest count from Redis
					long currentRedisCount = Long.parseLong(client.get(counterName));
					long newCount = currentRedisCount + maxIdsToFetch;

					// Increment by maxIdsToFetch and write back
					client.incrBy(counterName, maxIdsToFetch);

					// update ranges
					idRange = Range.closed(currentRedisCount, newCount);
					idCounter.set(currentRedisCount + 1);
					return;
				}

				throw new RuntimeException("Could not reserve a range due to hitting the max attempts of : "
						+ MAX_ATTEMPTS + " on CAS based operations");
			} catch (Exception e) {
				Throwables.propagate(e);
			}
		}
	}

	public static class Builder {
		private int maxIdsToFetch = 1000;
		private String counterName = "default";
		private Jedis client;

		public Builder(Jedis client) {
			this.client = client;
		}

		public Builder setCounterName(String counterName) {
			this.counterName = counterName;
			return this;
		}

		public Builder setMaxIdsToFetch(int maxIdsToFetch) {
			this.maxIdsToFetch = maxIdsToFetch;
			return this;
		}

		public IdGenerator build() {
			return new RedisGenerator(client, counterName, maxIdsToFetch);
		}
	}
}