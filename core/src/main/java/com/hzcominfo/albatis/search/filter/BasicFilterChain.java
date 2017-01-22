package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class BasicFilterChain<Q, R> implements FilterChain<Q, R> {

	private List<Filter<Q, R>> filterList = new ArrayList<>(4);
	private FilterChain<Q, R> nextChain;
	private volatile int index;

	@Override
	public void doFilter(Q query, R response) throws SearchAPIException {
		if (index == filterList.size()) { return; }
		if (nextChain == null) {
			nextChain = this;
		}
		Filter<Q, R> filter = filterList.get(index);
		index++;
		filter.doFilter(query, response, nextChain);
	}

	@Override
	public FilterChain<Q, R> add(Filter<Q, R> filter) {
		filterList.add(filter);
		return this;
	}

	/**
	 * Clone 方法 用于加速相同的对象的产生，减少在反射上的消耗 内部需调用Filter的Clone
	 * 
	 * @return
	 */
	@Override
	@SuppressWarnings("unchecked")
	public FilterChain<Q, R> clone() {
		BasicFilterChain<Q, R> filterChain = null;
		try {
			filterChain = (BasicFilterChain<Q, R>) super.clone();
			filterChain.nextChain = filterChain;
			filterChain.index = 0;
			// for(Filter filter :filterList){
			// if(filter != null){
			// filterChain.filterList.add((Cloneable)filter.clone());
			// }
			// }
			filterChain.filterList = filterList; // Filter 本身应该是幂等调用的
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return filterChain;
	}
}
