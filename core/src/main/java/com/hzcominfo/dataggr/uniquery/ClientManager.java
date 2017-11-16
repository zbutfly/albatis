package com.hzcominfo.dataggr.uniquery;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.io.URISpec;

public class ClientManager {
//	private final static Map<URISpec, Client> clients = new ConcurrentHashMap<>();

	// 资源创建
	public static Client getConnection(URISpec uriSpec) {
//		clients.compute(uriSpec, (u, c) -> null == c ? newClient(uriSpec):c);
		return new Client(uriSpec);
	}

	// 资源池
	private void cacheConnection(URISpec uriSpec, Client conn) {
		// TODO
	}

	// 资源释放
	public void releaseConnection(Client client) {
		// TODO
	}
}
