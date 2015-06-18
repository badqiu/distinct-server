package com.github.distinct_server;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.UnknownHostException;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.distinct_server.api.Constants;
import com.github.distinct_server.api.DistinctService;
import com.github.distinct_server.api.DistinctService.Processor;
import com.github.distinct_server.util.SpringContext;
import com.github.rapid.common.util.JVMUtil;
import com.github.rapid.common.util.PropertiesHelper;

public class Server {
	private static Logger logger = LoggerFactory.getLogger(Server.class);
	
	private int port = Constants.DEFAULT_PORT;
	
	public Server() {
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				logger.error("uncaughtException,Thread:"+t+" cuase:"+e,e);
			}
		});
	}
	
	public Server(int port) {
		this.port = port;
	}

	public void startServer() throws TTransportException, UnknownHostException {
		logger.info("start DistinctService on port:"+port);
		JVMUtil.lockFileForOnlyProcess("distinct-service-port-"+port);
		
		final DistinctService.Iface service = SpringContext.getBean("distinctService",DistinctService.Iface.class);
		TServerTransport serverTransport = new TServerSocket(port);
		
		startProcessor(new Processor(service), serverTransport);
	}

	private void startProcessor(TProcessor processor,TServerTransport serverTransport) {
		Factory portFactory = new TBinaryProtocol.Factory(true, true);

		Args args = new Args(serverTransport);
		args.maxWorkerThreads(2000);
		args.minWorkerThreads(8);
		args.processor(processor);
		args.protocolFactory(portFactory);
		
		TServer server = new TThreadPoolServer(args); // 有多种server可选择
//		server.setServerEventHandler(new TServerEventHandlerImpl());
		logger.info("start processor:"+processor+" thrift server");
		server.serve();
	}

	public static void main(String[] args) throws TTransportException, UnknownHostException {
		PropertiesHelper properties = new PropertiesHelper(System.getProperties());
		Server server = new Server(properties.getInt("port", Constants.DEFAULT_PORT));
//		server.startNoBlockServer();
		server.startServer();
	}
}