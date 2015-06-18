package com.github.distinct_server.client;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.github.distinct_server.api.BloomFilterRequest;
import com.github.distinct_server.api.Constants;
import com.github.distinct_server.api.DistinctService;
import com.github.distinct_server.api.DistinctService.Client;
import com.github.distinct_server.api.DistinctService.Iface;
import com.github.distinct_server.api.RemoteException;

class ThriftDistinctServiceClient extends GenericObjectPoolConfig implements Iface,InitializingBean,DisposableBean{
	private static Logger logger = LoggerFactory.getLogger(ThriftDistinctServiceClient.class);
	
	private String host;
	private int port = Constants.DEFAULT_PORT;

	private String username;
	private String password;

	private int clientPoolSize = 2;
	private int connectionTimeout = 0;
	private ObjectPool<DistinctService.Client> clientPool;
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getClientPoolSize() {
		return clientPoolSize;
	}

	public void setClientPoolSize(int clientPoolSize) {
		this.clientPoolSize = clientPoolSize;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	@Override
	public void destroy() throws Exception {
		if(clientPool != null) {
			clientPool.clear();
		}
	}
	
	public void afterPropertiesSet() throws Exception {
		if(StringUtils.isBlank(host)) throw new IllegalStateException("host must be not empty");
		if(port <= 0) throw new IllegalArgumentException("port > 0 must be true");
		if(clientPoolSize <= 0) throw new IllegalArgumentException("clientPoolSize > 0 must be true");
		
		clientPool = new GenericObjectPool<DistinctService.Client>(new ClientPoolableObjectFactory(),this);
		logger.info("init end,server="+host+":"+port+" clientPoolSize:"+clientPoolSize+" username:"+username);
	}
	
	@Override
	public int bloomFilterNotContainsCountAndAdd(BloomFilterRequest request,String vhost,
			String bloomfilterName) {
		Client client = borrowObject();
		try {
			int result = client.bloomFilterNotContainsCountAndAdd(request,vhost,bloomfilterName);
			returnObject(client);
			return result;
		} catch (RemoteException e) {
			returnObject(client);
			throw new RuntimeException("bloomFilterNotContainsCountAndAdd error",e);
		} catch(Exception e) {
			invalidateObject(client);
			throw new RuntimeException("bloomFilterNotContainsCountAndAdd error",e);
		}
	}
	
	@Override
	public Map<String, Integer> batchBloomFilterNotContainsCountAndAdd(
			List<BloomFilterRequest> requestList,String vhost,
			String bloomfilterName)  {
		Client client = borrowObject();
		try {
			Map<String,Integer> result = client.batchBloomFilterNotContainsCountAndAdd(requestList,vhost,bloomfilterName);
			returnObject(client);
			return result;
		} catch (RemoteException e) {
			returnObject(client);
			throw new RuntimeException("batchBloomFilterNotContainsCountAndAdd error",e);
		} catch(Exception e) {
			invalidateObject(client);
			throw new RuntimeException("batchBloomFilterNotContainsCountAndAdd error",e);
		}
	}
	
	@Override
	public void selectVhost(String vhost) {
		Client client = borrowObject();
		try {
			client.selectVhost(vhost);
			returnObject(client);
		} catch (RemoteException e) {
			returnObject(client);
			throw new RuntimeException("selectVhost error,vhost:"+vhost,e);
		} catch(Exception e) {
			invalidateObject(client);
			throw new RuntimeException("selectVhost error,vhost:"+vhost,e);
		}
	}
	
	@Override
	public void login(String username, String password)  {
		Client client = borrowObject();
		try {
			client.login(username,password);
			returnObject(client);
		} catch (RemoteException e) {
			returnObject(client);
			throw new RuntimeException("login error,username:"+username,e);
		} catch(Exception e) {
			invalidateObject(client);
			throw new RuntimeException("login error,username:"+username,e);
		}
	}
	
	@Override
	public String ping() throws RemoteException, TException {
		Client client = borrowObject();
		try {
			String result = client.ping();
			returnObject(client);
			return result;
		} catch (RemoteException e) {
			returnObject(client);
			throw new RuntimeException(e);
		} catch(Exception e) {
			invalidateObject(client);
			throw new RuntimeException(e);
		}
	}

	private void invalidateObject(Client client) {
		try {
			clientPool.invalidateObject(client);
		} catch (Exception e) {
			throw new RuntimeException("invalidateObject error",e);
		}
	}
	
	private void returnObject(Client client) {
		try {
			clientPool.returnObject(client);
		} catch (Exception e) {
			throw new RuntimeException("returnObject error",e);
		}
	}

	private Client borrowObject() {
		try {
			return clientPool.borrowObject();
		} catch (Exception e) {
			throw new RuntimeException("borrowObject error",e);
		}
	}
	
	private class ClientPoolableObjectFactory extends BasePooledObjectFactory<DistinctService.Client> {
		final Map<Client,TTransport> clientTTransportMap = new Hashtable<DistinctService.Client, TTransport>();
		public DistinctService.Client makeClient() throws Exception {
			TTransport transport = new TSocket(host, port,connectionTimeout);
			TProtocol protocol = new TBinaryProtocol(transport);
			transport.open();
			DistinctService.Client client = new DistinctService.Client(protocol);
			client.login(username, password);
			
			Assert.isTrue(validateObject(client),"client ping() error");
			
			clientTTransportMap.put(client, transport);
			logger.info("connected_to_server:"+host+":"+port+" by username:"+username+" clientPool.numActive:"+clientPool.getNumActive()+" clientPool.numIdle:"+clientPool.getNumIdle()+" clientTTransportMap.size:"+clientTTransportMap.size());
			return client;
		}
		
		public void destroyObject(DistinctService.Client obj) throws Exception {
			TTransport transport = clientTTransportMap.remove(obj);
			if(transport != null) {
				logger.info("destroyObject() closed_transport, server:"+host+":"+port+" by username:"+username+" clientPool.numActive:"+clientPool.getNumActive()+" clientPool.numIdle:"+clientPool.getNumIdle()+" clientTTransportMap.size:"+clientTTransportMap.size());
				transport.close();
			}
		}
		
		public boolean validateObject(Client obj) {
			String ping = null;
			try {
				ping = obj.ping();
				if(Constants.PING_RESPONSE.equals(ping)) {
					return true;
				}
				return false;
			} catch (Exception e) {
				ping = e.toString();
				return false;
			}finally {
				logger.info("validateObject,MessageBrokerService.Client ping() "+host+" and get response:"+ping);
			}
		}

		@Override
		public PooledObject<Client> makeObject() throws Exception {
			return new DefaultPooledObject(create());
		}

		@Override
		public Client create() throws Exception {
			return makeClient();
		}

		@Override
		public PooledObject<Client> wrap(Client obj) {
			return new DefaultPooledObject(obj);
		}
	}
}
