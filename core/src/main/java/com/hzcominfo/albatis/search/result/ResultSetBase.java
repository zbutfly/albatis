package com.hzcominfo.albatis.search.result;

import java.util.List;
import java.util.Map;

public interface ResultSetBase extends java.sql.ResultSet {
	
	List<Map<String, Object>> getResultMapList();
	
	void setResultMapList(List<Map<String, Object>> resultMapList);
}
