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

/**
 * Full example docs here: https://github.com/evertrue/monotone#zookeeper-support
 * 
 * This implementation is configurable as to its performance characteristics. This is driven off of the {@code #maxIdsToFetch}
 * parameter. This param dictates how many ids to cache in its range before having to reach out to ZK again. 
 * 
 * An instance of this class should be constructed for each unique counter you wish to have. If you wanted two counters,
 * one for <code>User</code> and one for <code>Product</code>, you would construct two instances, each with their own
 * corresponding counter names. 
 * 
 * Once a range has been "leased" by an instance of <code>ZKGenerator</code>, it cannot be obtained again. In other words,
 * if you instantiate this object, use 5 ids of a 1000, restart your server, 955 of those ids cannot be issued. Given this
 * behavior, you will not want to fetch too aggressively.
 * 
 * This object is thread safe and is intended to be used as a singleton.
 * 
 * @author mark
 */
public class ZKGenerator implements IdGenerator {
	private Range<Long> idRange;
	private final AtomicLong idCounter;
	private int maxIdsToFetch;
	private DistributedAtomicLong zkCounter;
	private int maxAttempts;

	private ZKGenerator(DistributedAtomicLong zkCounter, long seed,  String rootPath, String counterName, int maxIdsToFetch, int maxAttempts) {
		this.idCounter = new AtomicLong(0);
		this.idRange = Range.closedOpen(0l, 0l);
		this.maxIdsToFetch = maxIdsToFetch;
		this.maxAttempts = maxAttempts;

		this.zkCounter = zkCounter;

		initValueInZkIfNeeded(seed);
	}

	private void initValueInZkIfNeeded(long seed) {
		try {
			if (this.zkCounter.get().postValue() == 0) {
				this.zkCounter.trySet(seed);
			}
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}

	/**
	 * In accordance to the {@link IdGenerator} interface, generates a unique integer in a monotonically increasing
	 * fashion. This method will first look to see if its local range cache satisfies the id generation before
	 * doing a more expensive call to ZK which will refresh the local id range cache.
	 * 
	 * @return The next unique, monotonically increasing integer for the configured counter name.
	 */
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
		private long seed = 0L;

		public Builder(CuratorFramework client) {
			this.client = client;
		}

		/**
		 * @param counterName The name chosen here will be appended to the {@link #rootPath} to make up a full
		 * ZK path for storage. If the root path is <code>/id_gen/my_app</code> and your counter name is 
		 * <code>my_counter</code>, the resolved path will be <code>/id_gen/my_app/my_counter</code>.
		 * 
		 * This value cannot be <code>null</code>
		 * 
		 * @return
		 */
		public Builder setCounterName(String counterName) {
			Preconditions.checkNotNull(rootPath, "counterName cannot be null");

			this.counterName = counterName;
			return this;
		}

		/**
		 * @param rootPath The ZK path to be used for your counters. All {@link #counterName}'s will append off this
		 * path. This value cannot be <code>null</code>.
		 * @return
		 */
		public Builder setRootPath(String rootPath) {
			Preconditions.checkNotNull(rootPath, "rootPath cannot be null");

			this.rootPath = rootPath;
			return this;
		}

		/**
		 * @param maxIdsToFetch How many ids to cache in the local range. Needs to be greater than 0.
		 * @return
		 */
		public Builder setMaxIdsToFetch(int maxIdsToFetch) {
			Preconditions.checkArgument(maxIdsToFetch > 0, "maxIdsToFetch needs to be > 0");

			this.maxIdsToFetch = maxIdsToFetch;
			return this;
		}
		
		/**
		 * @param maxAttempts The number of attempts to lease a local range from ZK before throwing an exception. 
		 * Needs to be greater than 0.
		 * @return
		 */
		public Builder setMaxAttempts(int maxAttempts) {
			Preconditions.checkArgument(maxAttempts > 0, "maxAttempts needs to be > 0");

			this.maxAttempts = maxAttempts;
			return this;
		}

		/**
		 * @param seed The initial id to start from if there is no state in ZK.
		 * Needs to be greater than or equal to 0.
		 * @return
		 */
		public Builder setSeed(long seed) {
			Preconditions.checkArgument(seed >= 0, "seed needs to be >= 0");

			this.seed = seed;
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

			return new ZKGenerator(zkCounter, seed, rootPath, counterName, maxIdsToFetch, maxAttempts);
		}
	}
}