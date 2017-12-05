package com.hzcominfo.dataggr.uniquery.es5;

import static com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVistor.AGGRS_SUFFIX;

import java.util.ArrayList;
import java.util.HashMap;
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
		SearchResponse sr = (SearchResponse)searchResponse;
		JsonObject json = new Gson().fromJson(sr.toString(), JsonObject.class);
		if (json.keySet().contains("aggregations")) {
			JsonObject results = json.getAsJsonObject("aggregations");
			if (results.keySet().size() == 1) return (T) facet(results);
			else return (T) multiFacet(results);
		} else {
			SearchHits hits = sr.getHits();
			SearchHit[] hitList = hits.getHits();
			if (hitList == null || hitList.length < 1) return (T) count(hits);
			return (T) common(hitList);
		}
	}
	
	private Long count(SearchHits hits) {
		return hits.totalHits;
	}
	
	private List<Map<String, Object>> common(SearchHit[] hitList) {
		List<Map<String, Object>> resultMap = new ArrayList<>();
		for (SearchHit hit : hitList) {
			if (!hit.hasSource()) continue;
			Map<String, Object> map = new HashMap<>();
			Map<String, Object> sourceMap = hit.getSourceAsMap();
			for(String key : sourceMap.keySet()) {
				map.put(key, sourceMap.get(key));
			}
			resultMap.add(map);
		}
		return resultMap;
	}
	
	private static List<Map<String, Object>> facet(JsonObject results) {
		List<Map<String, Object>> mapList = new ArrayList<>();
		for (String result : results.keySet()) {
			JsonElement element = results.get(result);
			if (element.isJsonObject()) {
				analyseResultJson(element.getAsJsonObject(), mapList, result.split(AGGRS_SUFFIX)[0], new HashMap<>());
			}
			else continue;
		}
		return mapList;
	}
	
	private static List<List<Map<String, Object>>> multiFacet(JsonObject results) {
		List<List<Map<String, Object>>> multiList = new ArrayList<>();
		for (String result : results.keySet()) {
			JsonElement element = results.get(result);
			if (element.isJsonObject()) {
				List<Map<String, Object>> mapList = new ArrayList<>();
				analyseResultJson(element.getAsJsonObject(), mapList, result.split(AGGRS_SUFFIX)[0], new HashMap<>());
				multiList.add(mapList);
			}
			else continue;
		}
		return multiList;
	}
	
	private static void analyseResultJson(JsonObject jsonObject, List<Map<String, Object>> mapList, String val, 
			Map<String, Object> map) {
		if (jsonObject.keySet().contains("buckets")) {
			JsonElement buckets = jsonObject.get("buckets");
			if (buckets.isJsonArray())
				for (JsonElement element : buckets.getAsJsonArray())
					analyseResultJson(element.getAsJsonObject(), mapList, val, map);
		} else if (jsonObject.keySet().stream().filter(s -> s.endsWith(AGGRS_SUFFIX)).count() > 0) {
			List<String> valList = jsonObject.keySet().stream().filter(s -> s.endsWith(AGGRS_SUFFIX)).collect(Collectors.toList());
			for (String object : jsonObject.keySet()) {
				JsonElement element = jsonObject.get(object);
				if (element.isJsonObject())
					analyseResultJson(element.getAsJsonObject(), mapList, valList.get(0).split(AGGRS_SUFFIX)[0], map);
				else if ("key".equals(object)) {
						map.put(val, element);
				} else map.put("doc_count".equals(object) ? "count":object, element);
			}
		} else {
			Map<String, Object> subMap = new HashMap<>(map);
			for (String object : jsonObject.keySet()) {
				JsonElement element = jsonObject.get(object);
				if (element.isJsonObject()) {
					JsonObject funcObject = element.getAsJsonObject();
					subMap.put(object, funcObject.get("value"));
				} else if ("key".equals(object)) {
					subMap.put(val, element);
				} else subMap.put("doc_count".equals(object) ? "count":object, element);
			}
			mapList.add(subMap);
		}
	}
}
