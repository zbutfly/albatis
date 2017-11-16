package com.hzcominfo.dataggr.uniquery;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.utils.ExceptionUtil;

import net.butfly.albacore.io.URISpec;

public class Client implements AutoCloseable {
	private final static Map<URISpec, Connection> connections = new ConcurrentHashMap<>();
	private final static Map<URISpec, Adapter> adapters = new ConcurrentHashMap<>();

	private final URISpec uriSpec;
	private Connection conn;
	private Adapter adapter;

	public Client(URISpec uriSpec) {
		this.uriSpec = uriSpec;
		this.adapter = Adapter.adapt(uriSpec);
		try {
			this.conn = connect(uriSpec);
		} catch (Exception e) {
			ExceptionUtil.runtime("connect error", e);
		}
	}
	
	public Connection connect(URISpec uriSpec) {
		return connections.compute(uriSpec, (u, c) -> null == c ? newConnection(uriSpec):c);
	}
	
	public Connection newConnection(URISpec uriSpec) {
		try {
			return Connection.connect(uriSpec);
		} catch (Exception e) {
			ExceptionUtil.runtime("connect error: ", e);
			return null;
		}
	}
	
	public Adapter adapt(URISpec uriSpec) {
		return adapters.compute(uriSpec, (u, a) -> null == a ? Adapter.adapt(uriSpec):a);
	}

	public <T> T execute(String sql, Object...params) {
		try {
			JsonObject sqlJson = SqlExplainer.explain(sql);
			JsonObject tableJson = sqlJson.has("tables") ? sqlJson.getAsJsonObject("tables"):null;
			String table = tableJson != null && tableJson.has("table") ? tableJson.get("table").getAsString():null;
			Object query = adapter.queryAssemble(sqlJson, params);
			Object result = adapter.queryExecute(conn, query, table);
			return adapter.resultAssemble(result);
		} catch (Exception e) {
			ExceptionUtil.runtime("connect error", e);
		}
		return null;
	}

	@Override
	public void close() throws Exception {
		conn.close();
		connections.remove(uriSpec);
		adapters.remove(uriSpec);
	}
	
	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public Adapter getAdapter() {
		return adapter;
	}

	public void setAdapter(Adapter adapter) {
		this.adapter = adapter;
	}

	public URISpec getUriSpec() {
		return uriSpec;
	}
}
