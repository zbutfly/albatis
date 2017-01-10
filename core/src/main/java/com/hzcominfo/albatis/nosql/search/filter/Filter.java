package com.hzcominfo.albatis.nosql.search.filter;

import com.hzcominfo.albatis.nosql.search.exception.SearchAPIException;

/**
 * Created by lic
 *
 * @author lic
 */
@FunctionalInterface
public interface Filter<Q, R> {

    void doFilter(final Q before, final R after, FilterChain filterChain) throws SearchAPIException;
}
