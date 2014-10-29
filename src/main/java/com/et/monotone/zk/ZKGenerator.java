package com.et.monotone.zk;

import java.util.concurrent.atomic.AtomicLong;

import com.et.monotone.IdGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Range;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.framework.recipes.atomic.AtomicValue;
import com.netflix.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.netflix.curator.retry.RetryNTimes;

public class ZKGenerator implements IdGenerator {
	private Range<Long> idRange;
	private final AtomicLong idCounter;
	private int maxIdsToFetch;
	private DistributedAtomicLong zkCounter;
	private int maxAttempts;

	private ZKGenerator(DistributedAtomicLong zkCounter, String rootPath, String counterName, int maxIdsToFetch, int maxAttempts) {
		this.idCounter = new AtomicLong(0);
		this.idRange = Range.closedOpen(0l, 0l);
		this.maxIdsToFetch = maxIdsToFetch;
		this.maxAttempts = maxAttempts;

		this.zkCounter = zkCounter;

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
		Preconditions.checkNotNull(client, "A curartor client is required");
		Preconditions.checkArgument(client.getState() == CuratorFrameworkState.STARTED,
				"A curator client that is started is required");

		return new Builder(client);
	}

	private void refreshAndSetNewRange() {
		synchronized (idCounter) {
			if (idRange.contains(idCounter.get())) {
				return;
			}

			try {
				for (int i = 0; i < maxAttempts; i++) {
					// increment ZK counter
					AtomicValue<Long> result = zkCounter.add((long) maxIdsToFetch);

					if (result.succeeded()) {
						long endRange = result.postValue();
						long beginRange = Math.max(1, endRange - maxIdsToFetch);
						
						idRange = Range.closedOpen(beginRange, endRange);
						idCounter.set(beginRange);
						
						return;
					}
				}

				throw new RuntimeException("Could not reserve a range due to hitting the max attempts of : "
						+ maxAttempts + " on CAS based operations");
			} catch (Exception e) {
				Throwables.propagate(e);
			}
		}
	}

	public static class Builder {
		private int maxIdsToFetch = 1000;
		private int maxAttempts = 5;
		private String rootPath = "/monotone/id_gen";
		private String counterName = "default";
		private CuratorFramework client;
		private DistributedAtomicLong zkCounter;

		public Builder(CuratorFramework client) {
			this.client = client;
		}

		public Builder setCounterName(String counterName) {
			Preconditions.checkNotNull(rootPath, "counterName cannot be null");

			this.counterName = counterName;
			return this;
		}

		public Builder setRootPath(String rootPath) {
			Preconditions.checkNotNull(rootPath, "rootPath cannot be null");

			this.rootPath = rootPath;
			return this;
		}

		public Builder setMaxIdsToFetch(int maxIdsToFetch) {
			Preconditions.checkArgument(maxIdsToFetch > 0, "maxIdsToFetch needs to be > 0");

			this.maxIdsToFetch = maxIdsToFetch;
			return this;
		}
		
		public Builder setMaxAttempts(int maxAttempts) {
			Preconditions.checkArgument(maxAttempts > 0, "maxAttempts needs to be > 0");

			this.maxAttempts = maxAttempts;
			return this;
		}

		@VisibleForTesting
		Builder setDistributedAtomicLong(DistributedAtomicLong zkCounter) {
			this.zkCounter = zkCounter;
			return this;
		}

		public IdGenerator build() {
			RetryPolicy policy = new RetryNTimes(1, 100);
			if (zkCounter == null) {
				zkCounter = new DistributedAtomicLong(client, rootPath + "/" + counterName, policy);
			}

			return new ZKGenerator(zkCounter, rootPath, counterName, maxIdsToFetch, maxAttempts);
		}
	}
}