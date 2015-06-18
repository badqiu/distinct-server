package com.github.distinct_server.bloomfilter;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

public class MultiBloomFilterDb implements InitializingBean{
	private static final Logger logger = LoggerFactory.getLogger(MultiBloomFilterDb.class);
	private Map<String,BloomFilterDB> vhosts = new HashMap<String,BloomFilterDB>();
	
	private String dataDir;
	private int dumpIntervalSeconds = 1800;

	public String getDataDir() {
		return dataDir;
	}

	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}

	public int getDumpIntervalSeconds() {
		return dumpIntervalSeconds;
	}

	public void setDumpIntervalSeconds(int dumpIntervalSeconds) {
		this.dumpIntervalSeconds = dumpIntervalSeconds;
	}

	public BloomFilterDB getRequired(String vhost) {
		BloomFilterDB result = vhosts.get(vhost);
		if(result == null) {
			Assert.hasText(dataDir,"dataDir must be not blank");
			Assert.isTrue(dumpIntervalSeconds > 0,"dumpIntervalSeconds > 0 must be true");
			
			String baseDir = dataDir + "/" + vhost + "/";
			result = new BloomFilterDB(baseDir,dumpIntervalSeconds);
			logger.info("create bloomfilter vhost:"+vhost+" baseDir:"+baseDir+" dumpIntervalSeconds:"+dumpIntervalSeconds);
			vhosts.put(vhost, result);
		}
		Assert.notNull(result,"not found vhost by name:"+vhost);
		return result;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		logger.info("init end,dataDir:"+dataDir+" dumpIntervalSeconds:"+dumpIntervalSeconds);
	}
}
