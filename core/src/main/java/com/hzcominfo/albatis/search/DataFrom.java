/*
 * 文件名：DataSource.java
 * 版权：
 * 描述:搜索条件
 * 创建人： 郎敬翔
 * 修改时间：2016-11-24
 * 操作：创建
 */
package com.hzcominfo.albatis.search;

/**
 * @author ljx
 * @version 0.0.1
 * @see
 */
public interface DataFrom {
	public static final class Table implements DataFrom {
		public String tableName;

		public Table(String name) {
			this.tableName = name;
		}
	}

	public static final class DB implements DataFrom {
		public String dbName;

		public DB(String dbName) {
			this.dbName = dbName;
		}
	}

}
