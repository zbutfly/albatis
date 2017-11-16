package com.hzcominfo.dataggr.uniquery;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;

public class ClientManager {

	// 资源创建
	public static Client getConnection(URISpec uriSpec) {
		return new Client(uriSpec);
	}

	// 资源池
	private void cacheConnection(URISpec uriSpec, Client conn) {
		// TODO
	}

	// 资源释放
	public void releaseConnection(Connection conn) {
		// TODO
	}
}
