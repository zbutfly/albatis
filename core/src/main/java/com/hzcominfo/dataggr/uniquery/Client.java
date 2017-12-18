package com.hzcominfo.dataggr.uniquery;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.dto.Request;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;
import com.hzcominfo.dataggr.uniquery.utils.ExceptionUtil;

import net.butfly.albacore.io.URISpec;

public class Client implements AutoCloseable {
	private final static Map<String, Connection> connections = new ConcurrentHashMap<>();
	private final static Map<String, Adapter> adapters = new ConcurrentHashMap<>();

	private final URISpec uriSpec;
	private Connection conn;
	private Adapter adapter;

	public Client(URISpec uriSpec) {
		this.uriSpec = uriSpec;
		this.adapter = adapt(uriSpec);
		try {
			this.conn = connect(uriSpec);
		} catch (Exception e) {
			ExceptionUtil.runtime("connect error", e);
		}
	}
	
	public Connection connect(URISpec uriSpec) {
		return connections.compute(uriSpec.toString(), (u, c) -> null == c ? newConnection(uriSpec):c);
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
		return adapters.compute(uriSpec.getScheme(), (u, a) -> null == a ? Adapter.adapt(uriSpec):a);
	}

	/**
	 * common query and facet
	 * @param sql 
	 * @param params sql dynamic parameter
	 * @return
	 */
	public <T> T execute(String sql, Object...params) {
		try {
			JsonObject sqlJson = SqlExplainer.explain(sql, params);
			JsonObject tableJson = sqlJson.has("tables") ? sqlJson.getAsJsonObject("tables"):null;
			String table = tableJson != null && tableJson.has("table") ? tableJson.get("table").getAsString():null;
			Object query = adapter.queryAssemble(conn, sqlJson); 
			long start = System.currentTimeMillis();
			Object result = adapter.queryExecute(conn, query, table);
			System.out.println("query spends: " + (System.currentTimeMillis() - start) + "ms");
			return adapter.resultAssemble(result);
		} catch (Exception e) {
			ExceptionUtil.runtime("connect error", e);
		}
		return null;
	}
	
	/**
	 * multi facet
	 * @param sql
	 * @param facets group fields list
	 * @param params sql dynamic parameter
	 * @return
	 */
	public Map<String, ResultSet> execute(String sql, String[] facets, Object...params) {
		try {
			JsonObject sqlJson = SqlExplainer.explain(sql, params);
			JsonObject tableJson = sqlJson.has("tables") ? sqlJson.getAsJsonObject("tables"):null;
			String table = tableJson != null && tableJson.has("table") ? tableJson.get("table").getAsString():null;
			JsonArray facetArr = new JsonArray();
			Arrays.asList(facets).forEach(facetArr::add);
			sqlJson.add("multiGroupBy", facetArr);
			Object query = adapter.queryAssemble(conn, sqlJson); 
			long start = System.currentTimeMillis();
			Object result = adapter.queryExecute(conn, query, table);
			System.out.println("query spends: " + (System.currentTimeMillis() - start) + "ms");
			return adapter.resultAssemble(result);
		} catch (Exception e) {
			ExceptionUtil.runtime("connect error", e);
		}
		return null;
	}
	
	/**
	 * batch execute
	 * @param requests {String sql, Object...params}...
	 * @return
	 */
	public Map<String, ResultSet> batchExecute(Request...requests) {
		Map<String, ResultSet> resMap = new LinkedHashMap<>();
		for (Request request : requests) {
			ResultSet rs = execute(request.getSql(), request.getParams());
			resMap.put(request.getKey(), rs);
		}
		return resMap;
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
