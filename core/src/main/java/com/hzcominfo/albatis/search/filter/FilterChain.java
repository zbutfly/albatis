package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

/**
 * Defined as FilterChain in servlet
 *
 */
@FunctionalInterface
public interface FilterChain<Q, R>  {

    void doFilter(Q query,R response) throws SearchAPIException;
}
