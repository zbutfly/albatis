package com.hzcominfo.dataggr.uniquery.es5;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.Adapter;

import net.butfly.albatis.elastic.ElasticConnection;

public class Es5Adapter extends Adapter {
	final static String schema = "es";
	public Es5Adapter() {}

	@SuppressWarnings("unchecked")
	@Override
	public SearchRequestBuilder queryAssemble(Connection connection, JsonObject sqlJson) {
		TransportClient client = ((ElasticConnection) connection).client();
		SearchRequestBuilder requestBuilder = client.prepareSearch("");
		return new SearchRequestBuilderVistor(requestBuilder, sqlJson).get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public SearchResponse queryExecute(Connection connection, Object requestBuilder, String table) {
		return ((SearchRequestBuilder)requestBuilder).get();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> T resultAssemble(Object searchResponse) {
		SearchHits hits = ((SearchResponse)searchResponse).getHits();
		List<Map<String, Object>> resultMap = new ArrayList<>();
		if (hits.totalHits > 0) {
			SearchHit[] hitList = hits.getHits();
			for (SearchHit hit : hitList) {
				if (!hit.hasSource()) continue;
				Map<String, Object> map = new HashMap<>();
				Map<String, Object> sourceMap = hit.getSourceAsMap();
				for(String key : sourceMap.keySet()) {
					map.put(key, sourceMap.get(key));
				}
				resultMap.add(map);
			}
		}
		return (T) resultMap;
	}
}
