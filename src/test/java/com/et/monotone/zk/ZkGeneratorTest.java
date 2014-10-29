package com.et.monotone.zk;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.framework.recipes.atomic.AtomicValue;
import com.netflix.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.netflix.curator.utils.EnsurePath;

public class ZkGeneratorTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		long ID_SEED = 100;

		CuratorFramework mockCurator = Mockito.mock(CuratorFramework.class);
		EnsurePath mockEnsurePath = Mockito.mock(EnsurePath.class);
		DistributedAtomicLong mockZkLong = Mockito.mock(DistributedAtomicLong.class);
		AtomicValue<Long> mockAtomicValue = Mockito.mock(AtomicValue.class);

		Mockito.when(mockCurator.getState()).thenReturn(CuratorFrameworkState.STARTED);
		Mockito.when(mockCurator.newNamespaceAwareEnsurePath(Mockito.anyString())).thenReturn(mockEnsurePath);
		Mockito.when(mockEnsurePath.excludingLast()).thenReturn(mockEnsurePath);

		Mockito.when(mockZkLong.get()).thenReturn(mockAtomicValue);
		Mockito.when(mockAtomicValue.postValue()).thenReturn(ID_SEED);
		
		Mockito.when(mockZkLong.add(Mockito.anyLong())).thenReturn(mockAtomicValue);
		Mockito.when(mockAtomicValue.succeeded()).thenReturn(true);

		ZKGenerator mockGenerator = (ZKGenerator) ZKGenerator.newBuilder(mockCurator)
				.setDistributedAtomicLong(mockZkLong).build();
		long id = mockGenerator.nextId();

		Assert.assertEquals(1, id);
	}
}