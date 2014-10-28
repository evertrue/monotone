package com.et.monotone.zk;

import java.util.concurrent.atomic.AtomicLong;

import com.et.monotone.IdGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.Range;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.atomic.AtomicValue;
import com.netflix.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.netflix.curator.retry.RetryNTimes;

public class ZKGenerator implements IdGenerator {
	private final static int MAX_ATTEMPTS = 5;
	private Range<Long> idRange;
	private final AtomicLong idCounter;
	private int maxIdsToFetch;
	private DistributedAtomicLong zkCounter;

	private ZKGenerator(CuratorFramework client, String rootPath, String counterName, int maxIdsToFetch) {
		this.idCounter = new AtomicLong(0);
		this.idRange = Range.closedOpen(0l, 0l);
		this.maxIdsToFetch = maxIdsToFetch;

		RetryPolicy policy = new RetryNTimes(1, 100);
		this.zkCounter = new DistributedAtomicLong(client, rootPath + "/" + counterName, policy);
		
		initValueIfNeeded();
	}

	private void initValueIfNeeded() {
		try {
			if (this.zkCounter.get().postValue() == 0) {
				this.zkCounter.trySet(0l);
			}
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}
	
	@Override
	public long nextId() {
		if (idCounter.get() == 0) {
			refreshAndSetNewRange();
		}

		long nextId = idCounter.getAndIncrement();

		while (!idRange.contains(nextId)) {
			refreshAndSetNewRange();
			nextId = idCounter.getAndIncrement();
		}

		return nextId;
	}

	public static ZKGenerator.Builder newBuilder(CuratorFramework client) {
		return new Builder(client);
	}

	private void refreshAndSetNewRange() {
		synchronized (idCounter) {
			if (idRange.contains(idCounter.get())) {
				return;
			}

			try {
				for (int i = 0; i < MAX_ATTEMPTS; i++) {
					// Get latest count from ZK counter
					AtomicValue<Long> val = zkCounter.get();
					long currentZkCount = val.postValue();
					long newCount = currentZkCount + maxIdsToFetch;

					// Increment by maxIdsToFetch and write back
					AtomicValue<Long> result = zkCounter.compareAndSet(currentZkCount, newCount);

					// after success, update ranges
					if (result.succeeded()) {
						idRange = Range.closed(currentZkCount, newCount);
						idCounter.set(currentZkCount + 1);
						return;
					}
				}
			} catch (Exception e) {
				Throwables.propagate(e);
			}
		}
	}

	public static class Builder {
		private int maxIdsToFetch = 1000;
		private String rootPath = "/monotone/id_gen";
		private String counterName = "default";
		private CuratorFramework client;

		public Builder(CuratorFramework client) {
			this.client = client;
		}

		public Builder setCounterName(String counterName) {
			this.counterName = counterName;
			return this;
		}

		public Builder setRootPath(String rootPath) {
			this.rootPath = rootPath;
			return this;
		}

		public Builder setMaxIdsToFetch(int maxIdsToFetch) {
			this.maxIdsToFetch = maxIdsToFetch;
			return this;
		}

		public IdGenerator build() {
			return new ZKGenerator(client, rootPath, counterName, maxIdsToFetch);
		}
	}
}