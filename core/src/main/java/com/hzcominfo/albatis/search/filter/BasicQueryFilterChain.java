package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

import java.util.List;

/**
 *
 */
public abstract class BasicQueryFilterChain<Q,R> implements FilterChain<Q,R> {

    List<Filter<Q,R>> filterList;
    volatile int index;

    @Override
    public void doFilter(Q query, R response) throws SearchAPIException {

    }
}
