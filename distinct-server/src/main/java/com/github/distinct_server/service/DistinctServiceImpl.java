package com.github.distinct_server.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.github.distinct_server.api.BloomFilterRequest;
import com.github.distinct_server.api.RemoteException;
import com.github.distinct_server.bloomfilter.BloomFilter;
import com.github.distinct_server.bloomfilter.BloomFilterDB;
import com.github.distinct_server.bloomfilter.MultiBloomFilterDb;
import com.github.distinct_server.bloomfilter.PartitionBloomFilter;

public class DistinctServiceImpl implements com.github.distinct_server.api.DistinctService.Iface,InitializingBean{

	private static Logger logger = LoggerFactory.getLogger(DistinctServiceImpl.class);
	
	private MultiBloomFilterDb multiBloomFilterDb = null;
	
	private boolean serviceOn = true;
	
	public MultiBloomFilterDb getMultiBloomFilterDb() {
		return multiBloomFilterDb;
	}

	public void setMultiBloomFilterDb(MultiBloomFilterDb multiBloomFilterDb) {
		this.multiBloomFilterDb = multiBloomFilterDb;
	}

	@Override
	public int bloomFilterNotContainsCountAndAdd(BloomFilterRequest request, String vhost,String bloomfilterName) throws RemoteException, TException {
		assertServiceOn();
		Assert.hasText(vhost,"vhost must be not empty");
		Assert.hasText(bloomfilterName,"bloomfilterName must be not empty");
		
//		String vhost = request.getVhost();
		//FIXME 需要初始化好partition,避免创建bloomfilter耗时太多，现在没有同步，会导致耗时太多
		BloomFilter bloomFilter = lookupBloomFilter(request, vhost,bloomfilterName);
		synchronized(bloomFilter) {
			return bloomFilter.notContainsCountAndAdd("", request.getKeys());
		}
	}

	private synchronized BloomFilter lookupBloomFilter(BloomFilterRequest request,String vhost, String bloomfilterName) {
		BloomFilterDB db = multiBloomFilterDb.getRequired(vhost);
		PartitionBloomFilter partitionBloomFilter = db.get(bloomfilterName);
		BloomFilter bloomFilter = partitionBloomFilter.getBloomFilter(request.getBloomfilterPartition());
		return bloomFilter;
	}

	private void assertServiceOn() {
		if(serviceOn) return;
		
		throw new IllegalStateException("serviceOn=false,service alread stop");
	}

	@Override
	public Map<String,Integer> batchBloomFilterNotContainsCountAndAdd(List<BloomFilterRequest> requests,String vhost,
			String bloomfilterName) throws RemoteException, TException {
		assertServiceOn();
		
		Map<String,Integer> result = new HashMap();
		for(BloomFilterRequest request : requests) {
			int count = bloomFilterNotContainsCountAndAdd(request,vhost,bloomfilterName);
			result.put(request.getGroup(), count);
		}
		return result;
	}
	
	@Override
	public void selectVhost(String vhost) throws RemoteException, TException {
		//throw new RuntimeException("not yet impl");
	}

	@Override
	public void login(String username, String password) throws RemoteException,TException {
		//throw new RuntimeException("not yet impl");
	}

	@Override
	public String ping() throws RemoteException, TException {
		return "PONG";
	}
	
	public void addDumpAllDataShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				serviceOn = false;
				logger.info("start exec dump all data on ShutdownHook");
				if(multiBloomFilterDb != null) {
					multiBloomFilterDb.dumpAll();
				}
				logger.info("exec end dump all data on ShutdownHook");
			}
		});
		logger.info("exec addDumpAllDataShutdownHook() end");
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		addDumpAllDataShutdownHook();
	}

}
