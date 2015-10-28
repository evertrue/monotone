package com.et.monotone.zk;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.Range;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.framework.recipes.atomic.AtomicValue;
import com.netflix.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.netflix.curator.utils.EnsurePath;

public class ZkGeneratorTest {
	private CuratorFramework mockCurator;
	private EnsurePath mockEnsurePath;
	private DistributedAtomicLong mockZkLong;
	private AtomicValue<Long> mockAtomicValue;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		mockCurator = Mockito.mock(CuratorFramework.class);
		mockEnsurePath = Mockito.mock(EnsurePath.class);
		mockZkLong = Mockito.mock(DistributedAtomicLong.class);
		mockAtomicValue = Mockito.mock(AtomicValue.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSingleIncrement() throws Exception {
		long ID_SEED = 10;

		setupExpectationsForZkClientSetup();

		setupExpectationsForInitialSyncOfRemoteCounter(ID_SEED);

		Mockito.when(mockZkLong.add(Mockito.anyLong())).thenReturn(mockAtomicValue);
		Mockito.when(mockAtomicValue.succeeded()).thenReturn(true);

		ZKGenerator mockGenerator = (ZKGenerator) ZKGenerator.newBuilder(mockCurator)
				.setDistributedAtomicLong(mockZkLong).build();
		long id = mockGenerator.nextId();

		Assert.assertEquals(1, id);
	}

	/**
	 * Test the boundary condition of reaching out to ZK and reestablishing the high 
	 * water mark.
	 */
	@Test
	public void testRemoteIncrement() throws Exception {
		Range<Long> expectedRange = Range.closedOpen(100l, 104l);
		int INITIAL_REMOTE_COUNT = 100;
		int MAX_IDS_TO_FETCH = 2;
		int MAX_ITERATIONS = 3;

		setupExpectationsForZkClientSetup();
		setupExpectationsForInitialSyncOfRemoteCounter(INITIAL_REMOTE_COUNT);

		AtomicValue<Long> mockAtomicValue2 = Mockito.mock(AtomicValue.class);
		Mockito.when(mockZkLong.add((long) MAX_IDS_TO_FETCH)).thenReturn(mockAtomicValue2);
		Mockito.when(mockAtomicValue2.succeeded()).thenReturn(true);
		Mockito.when(mockAtomicValue2.postValue()).thenReturn(102l, 104l);

		ZKGenerator mockGenerator = (ZKGenerator) ZKGenerator.newBuilder(mockCurator)
				.setDistributedAtomicLong(mockZkLong).setMaxIdsToFetch(MAX_IDS_TO_FETCH).build();

		for (int i = 0; i <= MAX_ITERATIONS; i++) {
			long id = mockGenerator.nextId();
			Assert.assertTrue(expectedRange.contains(id));
		}
	}

	@Test
	public void testSeededZkGenerator() throws Exception {
		long SEED = 5;
		int MAX_IDS_TO_FETCH = 2;

		setupExpectationsForZkClientSetup();

		AtomicValue mockInitialValue = Mockito.mock(AtomicValue.class);
		Mockito.when(mockInitialValue.postValue()).thenReturn(0L);
		Mockito.when(mockZkLong.get()).thenReturn(mockInitialValue);

		ZKGenerator mockGenerator = (ZKGenerator) ZKGenerator.newBuilder(mockCurator)
				.setDistributedAtomicLong(mockZkLong).setMaxIdsToFetch(MAX_IDS_TO_FETCH).setSeed(SEED).build();

		ArgumentCaptor<Long> seedCaptor = ArgumentCaptor.forClass(Long.class);
		Mockito.verify(mockZkLong).trySet(seedCaptor.capture());
		long capturedSeed = seedCaptor.getValue();

		Assert.assertEquals(SEED, capturedSeed);

		setupExpectationsForInitialSyncOfRemoteCounter(capturedSeed + MAX_IDS_TO_FETCH);
		Mockito.when(mockZkLong.add(Mockito.anyLong())).thenReturn(mockAtomicValue);
		Mockito.when(mockAtomicValue.succeeded()).thenReturn(true);

		long id = mockGenerator.nextId();

		Assert.assertEquals(SEED, id);
	}

	@Test
	public void testSeedIsNotUsedIfZkGeneratorHasExistingValue() throws Exception {
		long EXISTING_VALUE = 7;
		long SEED = 5;
		int MAX_IDS_TO_FETCH = 2;

		setupExpectationsForZkClientSetup();

		AtomicValue mockInitialValue = Mockito.mock(AtomicValue.class);
		Mockito.when(mockInitialValue.postValue()).thenReturn(EXISTING_VALUE);
		Mockito.when(mockZkLong.get()).thenReturn(mockInitialValue);

		ZKGenerator mockGenerator = (ZKGenerator) ZKGenerator.newBuilder(mockCurator)
				.setDistributedAtomicLong(mockZkLong).setMaxIdsToFetch(MAX_IDS_TO_FETCH).setSeed(SEED).build();

		setupExpectationsForInitialSyncOfRemoteCounter(EXISTING_VALUE + MAX_IDS_TO_FETCH);
		Mockito.when(mockZkLong.add(Mockito.anyLong())).thenReturn(mockAtomicValue);
		Mockito.when(mockAtomicValue.succeeded()).thenReturn(true);

		long id = mockGenerator.nextId();

		Assert.assertEquals(EXISTING_VALUE, id);
	}

	private void setupExpectationsForZkClientSetup() {
		Mockito.when(mockCurator.getState()).thenReturn(CuratorFrameworkState.STARTED);
		Mockito.when(mockCurator.newNamespaceAwareEnsurePath(Mockito.anyString())).thenReturn(mockEnsurePath);
		Mockito.when(mockEnsurePath.excludingLast()).thenReturn(mockEnsurePath);
	}

	private void setupExpectationsForInitialSyncOfRemoteCounter(long INITIAL_REMOTE_COUNT) throws Exception {
		Mockito.when(mockZkLong.get()).thenReturn(mockAtomicValue);
		Mockito.when(mockAtomicValue.postValue()).thenReturn(INITIAL_REMOTE_COUNT);
	}
}