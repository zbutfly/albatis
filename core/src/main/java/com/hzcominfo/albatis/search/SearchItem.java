/*
 * 文件名：SearchItem.java
 * 版权：
 * 描述:所有查询的接口继承类
 * 创建人： 郎敬翔
 * 修改时间：2016-11-24
 * 操作：创建
 */
package com.hzcominfo.albatis.search;

import java.io.Serializable;

/**
 * Anything between "SELECT" and "FROM"<BR>
 * (that is, any column or expression etc to be retrieved with the query)
 *
 * @author ljx
 * @date 2016-11-24
 */
public interface SearchItem extends Serializable {
	public static final class Column implements SearchItem {
		public String column; // all field use"*"
		public String alias;

		public Column(String column) {
			super();
			this.column = column;
		}

		public SearchItem as(String alias) {
			this.alias = alias;
			return this;
		}
	}

	public static abstract class Function implements SearchItem {}

	public static abstract class ByFunctionColumn extends Function {
		public String column;
		public String alias;

		public ByFunctionColumn(String column) {
			this.column = column;
		}

		public SearchItem as(String alias) {
			this.alias = alias;
			return this;
		}
	}

	public static abstract class ByFunctionColumns extends Function {
		private String fristColumn;
		private String secondColumn;
		private String alias;

		public ByFunctionColumns(String fristColumn, String secondColumn) {
			this.fristColumn = fristColumn;
			this.secondColumn = secondColumn;
		}

		public void as(String alias) {
			this.alias = alias;
		}
	}

	public static final class Sum extends ByFunctionColumn {
		public Sum(String column) {
			super(column);
		}
	}

	public static final class Agv extends ByFunctionColumn {
		public Agv(String column) {
			super(column);
		}
	}

	public static final class Count extends ByFunctionColumn {
		public Count(String column) {
			super(column);
		}
	}

	public static final class Max extends ByFunctionColumn {
		public Max(String column) {
			super(column);
		}
	}

	public static final class Min extends ByFunctionColumn {
		public Min(String column) {
			super(column);
		}
	}

	public static final class Add extends ByFunctionColumns {
		public Add(String fristColumn, String secondColumn) {
			super(fristColumn, secondColumn);
		}
	}

	public static final class Sub extends ByFunctionColumns {
		public Sub(String fristColumn, String secondColumn) {
			super(fristColumn, secondColumn);
		}
	}

}