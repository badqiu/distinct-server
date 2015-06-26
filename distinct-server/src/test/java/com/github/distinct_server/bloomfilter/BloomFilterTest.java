package com.github.distinct_server.bloomfilter;

import static org.junit.Assert.*;

import org.junit.Test;

public class BloomFilterTest {

	@Test
	public void test() {
		BloomFilter bf = new BloomFilter(0.001,Integer.MAX_VALUE / 4);
		System.out.println(bf);
	}

}
