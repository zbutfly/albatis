/*
 * 文件名：query.java
 * 版权： 
 * 描述:所有查询的接口继承类
 * 创建人： 郎敬翔
 * 修改时间：2016-11-14
 * 操作：创建
 */

package com.hzcominfo.albatis.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hzcominfo.albatis.search.driver.Connection;
import com.hzcominfo.albatis.search.exception.SearchAPIException;
import com.hzcominfo.albatis.search.result.Result;

/**
 * 规定查询接口 query中迭代有逻辑关系
 *
 * @author ljx
 * @version 0.0.1
 * @see
 */
public interface Query extends Describe {
	public static final class SelectData implements Query {

		private List<SearchItem> searchItemList;

		private Criteria criteria;

		private Long limit;

		private Long skip;

		private List<DataFrom> db;

		private List<DataFrom> table;

		private List<OrderBy> orderByList;

		private Connection connection;

		private List<List<String>> groupby = new ArrayList<>();

		public SelectData(Connection connection) {
			this.connection = connection;
		}

		public Query select(SearchItem... searchItems) {
			searchItemList = Arrays.asList(searchItems);
			return this;
		}

		public Query groupBy(String... field) {
			groupby.add(Arrays.asList(field));
			return this;
		}

		public Query from(DataFrom... table) {
			this.table = Arrays.asList(table);
			return this;
		}

		public Query db(String... db) {
			this.db = Stream.of(db).map(dbName -> new DataFrom.DB(dbName)).collect(Collectors.toList());
			return this;
		}

		public Query where(Criteria criteria) {
			this.criteria = criteria;
			return this;
		}

		public Query limit(Long limit) {
			this.limit = limit;
			return this;
		}

		public Query skip(Long skip) {
			this.skip = skip;
			return this;
		}
		//
		// public query orderBy(OrderBy... orderBy) {
		// this.orderByList = Arrays.asList(orderBy);
		// return this;
		// }

		public Query orderBy(String field, OrderBy.Order order) {
			if (this.orderByList == null) {
				orderByList = new ArrayList<>();
			}
			orderByList.add(new OrderBy.Sort(field, order));
			return this;
		}

		@Override
		public Result execute() throws SearchAPIException, IOException {
			return connection.execute(new Action.selectAction(Action.ActionType.SELECT_DATA), this);
		}
	}

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