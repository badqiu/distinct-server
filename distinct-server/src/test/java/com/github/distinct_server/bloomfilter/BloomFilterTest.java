package com.github.distinct_server.bloomfilter;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.github.rapid.common.test.util.MultiThreadTestUtils;

public class BloomFilterTest {

	private long start = 0;
	BloomFilter bf;
	@Rule public TestName testName = new TestName();
	
	@Before
	public void before() {
		bf = new BloomFilter(0.001,Integer.MAX_VALUE / 4);
		start = System.currentTimeMillis();
	}
	
	@After
	public void after() {
		long end = System.currentTimeMillis();
		long cost = end - start;
		System.out.println(testName.getMethodName()+"() cost:"+cost);
	}
	
	@Test
	public void test() {
		BloomFilter bf = new BloomFilter(0.001,Integer.MAX_VALUE / 4);
		System.out.println(bf);
		
		bf = new BloomFilter(0.0001,Integer.MAX_VALUE / 2);
		System.out.println(bf);
	}

	
	@Test
	public void testMultiThread() throws InterruptedException {
		MultiThreadTestUtils.executeAndWait(100, new Runnable() {
			@Override
			public void run() {
				synchronized (bf) {
					int cnt = bf.notContainsCountAndAdd(Arrays.asList("1","2","3"));
					if(cnt > 0)
						System.out.println(cnt);
				}
			}
		});
		
//		System.out.println(bf.notContainsCountAndAdd(Arrays.asList("1","2","3")));
	}
	
}
