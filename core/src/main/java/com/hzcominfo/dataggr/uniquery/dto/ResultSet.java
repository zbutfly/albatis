package com.hzcominfo.dataggr.uniquery.dto;

import java.util.List;
import java.util.Map;

public class ResultSet {

	private long total;
	private List<Map<String, Object>> results;
	public long getTotal() {
		return total;
	}
	public void setTotal(long total) {
		this.total = total;
	}
	public List<Map<String, Object>> getResults() {
		return results;
	}
	public void setResults(List<Map<String, Object>> results) {
		this.results = results;
	}
	@Override
	public String toString() {
		return "ResultSet [total=" + total + ", results=" + results + "]";
	}
}
