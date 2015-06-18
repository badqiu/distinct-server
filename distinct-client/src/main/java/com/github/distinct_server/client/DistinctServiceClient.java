package com.github.distinct_server.client;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.github.distinct_server.api.BloomFilterRequest;
import com.github.distinct_server.api.RemoteException;

public class DistinctServiceClient implements InitializingBean,DisposableBean{
	private ThriftDistinctServiceClient client = new ThriftDistinctServiceClient();
	private String vhost;
	
	public String getVhost() {
		return vhost;
	}

	public void setVhost(String vhost) {
		this.vhost = vhost;
	}

	public String getHost() {
		return client.getHost();
	}

	public void setHost(String host) {
		client.setHost(host);
	}

	public int getPort() {
		return client.getPort();
	}

	public void setPort(int port) {
		client.setPort(port);
	}

	public String getUsername() {
		return client.getUsername();
	}

	public void setUsername(String username) {
		client.setUsername(username);
	}

	public String getPassword() {
		return client.getPassword();
	}

	public void setPassword(String password) {
		client.setPassword(password);
	}

	public int getClientPoolSize() {
		return client.getClientPoolSize();
	}

	public void setClientPoolSize(int clientPoolSize) {
		client.setClientPoolSize(clientPoolSize);
	}
	
	public int getConnectionTimeout() {
		return client.getConnectionTimeout();
	}

	public void setConnectionTimeout(int connectionTimeout) {
		client.setConnectionTimeout(connectionTimeout);
	}

	public String ping() throws RemoteException, TException {
		return client.ping();
	}

	public int bloomFilterNotContainsCountAndAdd(BloomFilterRequest request,String bloomfilterName) {
		return client.bloomFilterNotContainsCountAndAdd(request, vhost,
				bloomfilterName);
	}

	public Map<String, Integer> batchBloomFilterNotContainsCountAndAdd(List<BloomFilterRequest> requestList,String bloomfilterName) {
		return client.batchBloomFilterNotContainsCountAndAdd(requestList,vhost, bloomfilterName);
	}

	@Override
	public void destroy() throws Exception {
		client.destroy();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(client,"client must be not null");
		Assert.hasText(vhost,"vhost must be not empty");
		client.afterPropertiesSet();
	}
	
}
