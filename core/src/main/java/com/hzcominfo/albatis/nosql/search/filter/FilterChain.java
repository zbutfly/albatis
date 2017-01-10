package com.hzcominfo.albatis.nosql.search.filter;

import com.hzcominfo.albatis.nosql.search.exception.SearchAPIException;

/**
 * Defined as FilterChain in servlet
 */
@FunctionalInterface
public interface FilterChain<Q, R> {

    void doFilter(Q query, R response) throws SearchAPIException;
}
