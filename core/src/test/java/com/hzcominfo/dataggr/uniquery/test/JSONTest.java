package com.hzcominfo.dataggr.uniquery.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class JSONTest {

	public static void main(String[] args) {
		List<Map<String, Object>> mapList = new ArrayList<>();
		Map<String, Object> map1 = new HashMap<String, Object>();
		map1.put("name", "chenw");
		map1.put("age", "18");
		mapList.add(map1);
		
		Map<String, Object> map2 = new HashMap<String, Object>();
		map2.put("name", "chenw");
		map2.put("age", "18");
		mapList.add(map2);
		
		System.out.println(JSON.toJSONString(mapList, SerializerFeature.WriteMapNullValue));
	}
}
