package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

/**
 * Created by lic
 * @author lic
 */
public interface Filter<Q,R> {

    void doFilter(final Q before, final R after, FilterChain filterChain) throws SearchAPIException;
}
