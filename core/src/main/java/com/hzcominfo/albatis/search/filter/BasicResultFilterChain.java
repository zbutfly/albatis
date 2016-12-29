package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

import java.util.List;

/**
 * 单参数FilterChain
 *
 */
public abstract class BasicResultFilterChain<T> implements FilterChain<T,T> {


    List<Filter<T,T>> filterList;
    volatile  int index;

    @Override
    public void doFilter(T query, T response) throws SearchAPIException {

    }


}
