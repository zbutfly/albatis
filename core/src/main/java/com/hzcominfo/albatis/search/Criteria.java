/*
 * 文件名：Criteria.java
 * 版权：
 * 描述:搜索条件
 * 创建人： 郎敬翔
 * 修改时间：2016-11-24
 * 操作：创建
 */
package com.hzcominfo.albatis.search;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import com.hzcominfo.albatis.search.exception.SearchAPIError;

/**
 * @author ljx
 * @version 0.0.1
 * @see
 */

public interface Criteria extends Serializable {
	public static abstract class ByField<V> implements Criteria {
		private static final long serialVersionUID = -1L;
		public String field;

		public ByField(String field) {
			super();
			if (field == null) throw new SearchAPIError("field is not null");
			this.field = field;
		}
	}

	public static abstract class ByFieldValue<V> extends ByField<V> {
		private static final long serialVersionUID = -1L;
		public V value;

		public ByFieldValue(String field, V value) {
			super(field);
			this.value = value;
		}
	}

	public static final class Equal<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = -6072059360387978184L;

		public Equal(String field, V value) {
			super(field, value);
		}
	}

	public static final class NotEqual<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = -5946751264164934310L;

		public NotEqual(String field, V value) {
			super(field, value);
		}
	}

	public class Less<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = 4593053001556231013L;

		public Less(String field, V value) {
			super(field, value);
		}

	}

	public class Greater<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = 6841459886500231382L;

		public Greater(String field, V value) {
			super(field, value);
		}

	}

	public class LessOrEqual<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = -4045493244001924022L;
		public V value;

		public LessOrEqual(String field, V value) {
			super(field, value);
		}
	}

	public class GreaterOrEqual<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = 8679129438234499986L;
		public V value;

		public GreaterOrEqual(String field, V value) {
			super(field, value);
		}
	}

	public static final class In<V> extends ByField<V> {
		private static final long serialVersionUID = -3327669047546685341L;
		public Collection<V> values;

		public In(String field, Collection<V> values) {
			super(field);
			this.values = values;
		}

		@SafeVarargs
		public In(String field, V... value) {
			super(field);
			this.values = new HashSet<>(Arrays.asList(value));
		}
	}

	public static class And implements Criteria {
		private static final long serialVersionUID = -644453919882630263L;
		public List<Criteria> filters;

		public And(Criteria... filters) {
			super();
			this.filters = new ArrayList<>(Arrays.asList(filters));
		}
	}

	public static class Not implements Criteria {
		private static final long serialVersionUID = 6621724392062910751L;
		public List<Criteria> filters;

		public Not(Criteria... filters) {
			super();
			this.filters = new ArrayList<>(Arrays.asList(filters));
		}
	}

	public static final class Or implements Criteria {
		private static final long serialVersionUID = -6023895386145578847L;
		public List<Criteria> filters;

		public Or(Criteria... filters) {
			super();
			this.filters = new ArrayList<>(Arrays.asList(filters));
		}
	}

	public static final class Random implements Criteria {
		private static final long serialVersionUID = -2980235677478896288L;
		public float chance;

		public Random(float chance) {
			super();
			this.chance = chance;
		}
	}

	public static final class Regex extends ByField<String> implements Criteria {
		private static final long serialVersionUID = 4509935644112856045L;
		public Pattern regex;

		public Regex(String field, String regex) {
			super(field);
			this.regex = Pattern.compile(regex);
		}
	}

	/**
	 * For mongodb, $where: {javascript}
	 *
	 * @author zx
	 */
	public static final class Where extends ByFieldValue<String> implements Criteria {
		private static final long serialVersionUID = 44828739379557295L;

		public Where(String field, String where) {
			super(field, where);
		}

	}

	/**
	 * For mongodb, $type: int
	 *
	 * @author zx
	 */
	public static final class Type extends ByFieldValue<Integer> implements Criteria {
		private static final long serialVersionUID = 8237205268336851434L;

		public Type(String field, int type) {
			super(field, type);
		}
	}

	/**
	 * allfield search
	 *
	 * @param <V>
	 * @author ljx
	 */
	public static final class AllField<V> implements Criteria {
		private static final long serialVersionUID = 8237205268336851434L;
		public V value;

		public AllField(V v) {
			this.value = v;
		}
	}
}
