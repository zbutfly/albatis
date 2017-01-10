package com.hzcominfo.albatis.nosql.search.filter;

import java.util.ArrayList;
import java.util.List;

import com.hzcominfo.albatis.nosql.search.exception.SearchAPIException;

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

    public BasicFilterChain<Q, R> add(Filter<Q, R> filter) {
        filterList.add(filter);
        return this;
    }
}
