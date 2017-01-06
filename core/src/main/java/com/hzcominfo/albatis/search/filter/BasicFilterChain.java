package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class BasicFilterChain<Q, R> implements FilterChain<Q, R> {

    private List<Filter<Q, R>> filterList = new ArrayList<>(4);
    private FilterChain<Q, R> nextChain;
    private volatile int index;

    @Override
    public void doFilter(Q query, R response) throws SearchAPIException {
        if (index == filterList.size()) {
            return;
        }
        if (nextChain == null) {
            nextChain = this;
        }
        Filter<Q, R> filter = filterList.get(index);
        index++;
        filter.doFilter(query, response, nextChain);
    }

    @Override
    public FilterChain<Q, R> add(Filter<Q,R> filter) {
        filterList.add(filter);
        return this;
    }
}
