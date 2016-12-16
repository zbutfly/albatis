/*
 * 文件名：query.java
 * 版权： 
 * 描述:所有查询的接口继承类
 * 创建人： 郎敬翔
 * 修改时间：2016-11-14
 * 操作：创建
 */

package com.hzcominfo.albatis.search;

/**
 * 规定查询接口 query中迭代有逻辑关系
 *
 * @author ljx
 * @version 0.0.1
 * @see
 */
public interface Query extends Describe {
	public Query select(SearchItem... searchItems);

	public Query from(DataFrom... table);

	public Query db(String... db);

	public Query where(Criteria criteria);

	public Query limit(Long limit);

	public Query skip(Long skip);

	// public query orderBy(OrderBy... orderBy);

	public Query orderBy(String field, OrderBy.Order order);

	public Query groupBy(String... field);
}