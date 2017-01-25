package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

/**
 * Created by lic
 *
 * @author lic
 */
@FunctionalInterface
public interface Filter<Q, R> extends Cloneable {

	void doFilter(final Q before, final R after, FilterChain<Q, R> filterChain) throws SearchAPIException;
}
