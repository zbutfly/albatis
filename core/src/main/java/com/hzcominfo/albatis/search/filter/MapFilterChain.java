package com.hzcominfo.albatis.search.filter;

import java.util.Map;

/**
 * 限定于只能用于过滤Map数据的FilterChain 输入和输出都是Map
 */
@SuppressWarnings("rawtypes")
public class MapFilterChain<K extends Map, V extends Map> extends BasicFilterChain<K, V> {

}
