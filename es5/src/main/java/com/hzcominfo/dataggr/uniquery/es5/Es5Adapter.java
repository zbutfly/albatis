package com.hzcominfo.dataggr.uniquery.es5;

import static com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVistor.AGGRS_SUFFIX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.Adapter;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;

import net.butfly.albatis.elastic.ElasticConnection;

public class Es5Adapter extends Adapter {
	final static String schema = "es,elasticsearch";
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
		SearchResponse sr = (SearchResponse)searchResponse;
		JsonObject json = new Gson().fromJson(sr.toString(), JsonObject.class);
		if (json.keySet().contains("aggregations")) {
			JsonObject results = json.getAsJsonObject("aggregations");
			if (results.keySet().size() == 1) return (T) facet(results);
			else return (T) multiFacet(results);
		} else {
			SearchHits hits = sr.getHits();
			return (T) common(hits);
		}
	}
	
	private ResultSet common(SearchHits hits) {
		ResultSet rs = new ResultSet();
		List<Map<String, Object>> resultMap = new ArrayList<>();
		for (SearchHit hit : hits.getHits()) {
			if (!hit.hasSource()) continue;
			Map<String, Object> map = new HashMap<>();
			Map<String, Object> sourceMap = hit.getSourceAsMap();
			for(String key : sourceMap.keySet()) {
				map.put(key, sourceMap.get(key));
			}
			resultMap.add(map);
		}
		rs.setTotal(hits.totalHits);
		rs.setResults(resultMap);
		return rs;
	}
	
	private static ResultSet facet(JsonObject results) {
		ResultSet rs = new ResultSet();
		List<Map<String, Object>> mapList = new ArrayList<>();
		for (String result : results.keySet()) {
			JsonElement element = results.get(result);
			List<String> key = new ArrayList<>();
			if (element.isJsonObject()) {
				analyseResultJson(element.getAsJsonObject(), mapList, result.split(AGGRS_SUFFIX)[0], new HashMap<>(), key);
			}
			else continue;
		}
		rs.setResults(mapList);
		rs.setTotal(mapList.size());
		return rs;
	}
	
	private static Map<String, ResultSet> multiFacet(JsonObject results) {
		Map<String, ResultSet> rsMap = new LinkedHashMap<>();
		for (String result : results.keySet()) {
			JsonElement element = results.get(result);
			if (element.isJsonObject()) {
				ResultSet rs = new ResultSet();
				List<String> key = new ArrayList<>();
				List<Map<String, Object>> mapList = new ArrayList<>();
				String val = result.split(AGGRS_SUFFIX)[0];
				key.add(val);
				analyseResultJson(element.getAsJsonObject(), mapList, val, new HashMap<>(), key);
				rs.setResults(mapList);
				rs.setTotal(mapList.size());
				rsMap.put(key.toString(), rs);
			} else continue;
		}
		return rsMap;
	}
	
	private static void analyseResultJson(JsonObject jsonObject, List<Map<String, Object>> mapList, String val, 
			Map<String, Object> map, List<String> key) {
		if (jsonObject.keySet().contains("buckets")) {
			JsonElement buckets = jsonObject.get("buckets");
			if (buckets.isJsonArray())
				for (JsonElement element : buckets.getAsJsonArray())
					analyseResultJson(element.getAsJsonObject(), mapList, val, map, key);
		} else if (jsonObject.keySet().stream().filter(s -> s.endsWith(AGGRS_SUFFIX)).count() > 0) {
			List<String> valList = jsonObject.keySet().stream().filter(s -> s.endsWith(AGGRS_SUFFIX)).collect(Collectors.toList());
			for (String object : jsonObject.keySet()) {
				JsonElement element = jsonObject.get(object);
				if (element.isJsonObject()) {
					String nextVal = valList.get(0).split(AGGRS_SUFFIX)[0];
					key.add(nextVal);
					analyseResultJson(element.getAsJsonObject(), mapList, nextVal, map, key);
				}else if ("key".equals(object)) {
						map.put(val, val(element));
				} else map.put("doc_count".equals(object) ? "count":object, val(element));
			}
		} else {
			Map<String, Object> subMap = new HashMap<>(map);
			for (String object : jsonObject.keySet()) {
				JsonElement element = jsonObject.get(object);
				if (element.isJsonObject()) {
					JsonObject funcObject = element.getAsJsonObject();
					subMap.put(object, val(funcObject.get("value")));
				} else if ("key".equals(object)) {
					subMap.put(val, val(element));
				} else subMap.put("doc_count".equals(object) ? "count":object, val(element));
			}
			mapList.add(subMap);
		}
	}
}
