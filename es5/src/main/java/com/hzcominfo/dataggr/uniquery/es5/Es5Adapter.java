package com.hzcominfo.dataggr.uniquery.es5;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;

import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.Adapter;

public class Es5Adapter extends Adapter {
	final static String schema = "solr,zk:solr,solr:http";
	public Es5Adapter() {}

	@SuppressWarnings("unchecked")
	@Override
	public SearchRequestBuilder queryAssemble(JsonObject sqlJson, Object...params) {
		// TODO 
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public SearchResponse queryExecute(Connection connection, Object es5Params, String table) {
		// TODO
		return null;
	}
	
	@Override
	public <T> T resultAssemble(Object queryResponse) {
		return null;
	}
}
