// thrift --gen java  <xxxx.thrift>

namespace java com.github.distinct_server.api

struct BloomFilterRequest {
	1: string group, // 
	2: set<string> keys, // 
	3: required string bloomfilterPartition,
	// 3: required string vhost,
	// 4: required string bloomfilterName,
}

struct BloomFilterResponse {
	1: string group, // 
	2: i32 count, //
}

exception RemoteException {
  1: string errorCode,
  2: string message,
}


service DistinctService {
	/**
	 * 发送消息
	 * @param msg
	 */
	i32 bloomFilterNotContainsCountAndAdd(1:BloomFilterRequest request,2:string vhost,3:string bloomfilterName) throws (1:RemoteException e),
	map<string,i32> batchBloomFilterNotContainsCountAndAdd(1:list<BloomFilterRequest> requestList,2:string vhost,3:string bloomfilterName) throws (1:RemoteException e),
	
	void selectVhost(1:string vhost) throws (1:RemoteException e),

	/**
	 * 登陆
	 */
	void login(1:string username,2:string password) throws (1:RemoteException e),

	/**
	 * 心跳检查,返回字符串: PONG
	 */
	string ping() throws (1:RemoteException e),			
}

