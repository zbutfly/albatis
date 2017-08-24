package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

/**
 * Defined as FilterChain in servlet
 */
public interface FilterChain<Q, R> extends Cloneable {

	void doFilter(Q query, R response) throws SearchAPIException;

	FilterChain<Q, R> add(Filter<Q, R> filter);

	default FilterChain<Q, R> clone() {
		return null;
	}// 默认不输出clone
}
