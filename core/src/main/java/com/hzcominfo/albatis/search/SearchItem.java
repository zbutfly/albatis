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
		private static final long serialVersionUID = -5637227906358121541L;
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

	public static abstract class Function implements SearchItem {
		private static final long serialVersionUID = 7687835101998726424L;
	}

	public static abstract class ByFunctionColumn extends Function {
		private static final long serialVersionUID = 7362988191068875155L;
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
		private static final long serialVersionUID = -9146428976219684894L;
		protected String fristColumn;
		protected String secondColumn;
		protected String alias;

		public ByFunctionColumns(String fristColumn, String secondColumn) {
			this.fristColumn = fristColumn;
			this.secondColumn = secondColumn;
		}

		public void as(String alias) {
			this.alias = alias;
		}
	}

	public static final class Sum extends ByFunctionColumn {
		private static final long serialVersionUID = 4354379839273029859L;

		public Sum(String column) {
			super(column);
		}
	}

	public static final class Agv extends ByFunctionColumn {
		private static final long serialVersionUID = -2986042896070612288L;

		public Agv(String column) {
			super(column);
		}
	}

	public static final class Count extends ByFunctionColumn {
		private static final long serialVersionUID = 1583509643101663910L;

		public Count(String column) {
			super(column);
		}
	}

	public static final class Max extends ByFunctionColumn {
		private static final long serialVersionUID = -3639325065872389877L;

		public Max(String column) {
			super(column);
		}
	}

	public static final class Min extends ByFunctionColumn {
		private static final long serialVersionUID = -695357822984994532L;

		public Min(String column) {
			super(column);
		}
	}

	public static final class Add extends ByFunctionColumns {
		private static final long serialVersionUID = 2333194798260341262L;

		public Add(String fristColumn, String secondColumn) {
			super(fristColumn, secondColumn);
		}
	}

	public static final class Sub extends ByFunctionColumns {
		private static final long serialVersionUID = 4965593835763468543L;

		public Sub(String fristColumn, String secondColumn) {
			super(fristColumn, secondColumn);
		}
	}

}