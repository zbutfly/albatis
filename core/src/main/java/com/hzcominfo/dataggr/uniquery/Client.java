package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.utils.ExceptionUtil;

import net.butfly.albacore.io.URISpec;

public class Client implements AutoCloseable {

	private URISpec uriSpec;

	public Client(URISpec uriSpec) {
		this.setUriSpec(uriSpec);
	}

	public <T> T execute(String sql, Object...params) {
		try {
			Connection conn = Connection.connect(uriSpec);
			Adapter adapter = Adapter.adapt(uriSpec);
			JsonObject sqlJson = SqlExplainer.explain(sql);
			Object query = adapter.queryAssemble(sqlJson, params);
			Object result = adapter.queryExecute(conn, query);
			return adapter.resultAssemble(result);
		} catch (Exception e) {
			ExceptionUtil.runtime("connect error", e);
		}
		return null;
	}

	public URISpec getUriSpec() {
		return uriSpec;
	}

	public void setUriSpec(URISpec uriSpec) {
		this.uriSpec = uriSpec;
	}

	@Override
	public void close() throws Exception {
		
	}
}
