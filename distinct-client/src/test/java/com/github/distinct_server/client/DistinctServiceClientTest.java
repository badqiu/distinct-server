package com.github.distinct_server.client;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import com.github.distinct_server.api.BloomFilterRequest;

public class DistinctServiceClientTest {

	DistinctServiceClient client = new DistinctServiceClient();
	@Before
	public void setUp() throws Exception {
		client.setHost("localhost");
		client.setVhost("vhost_default");
		client.setClientPoolSize(20);
		client.afterPropertiesSet();
		assertEquals("PONG",client.ping());
		
	}
	@Test
	public void test() throws Exception {
		
		BloomFilterRequest req = new BloomFilterRequest();
//		req.setVhost("test");
//		req.setBloomfilterName("badqiu_bf");
		req.setBloomfilterPartition("2015-01-01");
		req.setGroup("game");
		req.setKeys(new HashSet(Arrays.asList("badqiu","jane")));
		int count = client.bloomFilterNotContainsCountAndAdd(req,"badqiu_bf");
		client.destroy();
		
//		assertEquals(2,count);
		
	}
	
	@Test
	public void testDestroy() throws Exception {
		client.destroy();
	}

}
