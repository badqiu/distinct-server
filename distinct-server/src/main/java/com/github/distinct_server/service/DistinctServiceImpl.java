package com.github.distinct_server.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.springframework.util.Assert;

import com.github.distinct_server.api.BloomFilterRequest;
import com.github.distinct_server.api.RemoteException;
import com.github.distinct_server.bloomfilter.BloomFilter;
import com.github.distinct_server.bloomfilter.BloomFilterDB;
import com.github.distinct_server.bloomfilter.MultiBloomFilterDb;
import com.github.distinct_server.bloomfilter.PartitionBloomFilter;

public class DistinctServiceImpl implements com.github.distinct_server.api.DistinctService.Iface{

	private MultiBloomFilterDb multiBloomFilterDb = null;
	
	public MultiBloomFilterDb getMultiBloomFilterDb() {
		return multiBloomFilterDb;
	}

	public void setMultiBloomFilterDb(MultiBloomFilterDb multiBloomFilterDb) {
		this.multiBloomFilterDb = multiBloomFilterDb;
	}

	@Override
	public int bloomFilterNotContainsCountAndAdd(BloomFilterRequest request, String vhost,
			String bloomfilterName) throws RemoteException, TException {
		Assert.hasText(vhost,"vhost must be not empty");
		Assert.hasText(bloomfilterName,"bloomfilterName must be not empty");
		
//		String vhost = request.getVhost();
		BloomFilterDB db = multiBloomFilterDb.getRequired(vhost);
		PartitionBloomFilter partitionBloomFilter = db.get(bloomfilterName);
		BloomFilter bloomFilter = partitionBloomFilter.getBloomFilter(request.getBloomfilterPartition());
		int result = bloomFilter.notContainsCountAndAdd("", request.getKeys());
		return result;
	}

	@Override
	public Map<String,Integer> batchBloomFilterNotContainsCountAndAdd(List<BloomFilterRequest> requests,String vhost,
			String bloomfilterName) throws RemoteException, TException {
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

}
