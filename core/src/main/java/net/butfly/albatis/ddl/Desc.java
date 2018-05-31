package net.butfly.albatis.ddl;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;

@SuppressWarnings("unchecked")
public abstract class Desc<D extends Desc<D>> {
	protected final Map<String, Object> attrs = Maps.of();
	public static final String FORMAT = "format";
	public static final String VALIDATE = "validate";
	public static final String NESTED = "nested";
	public static final String SEGMODE = "segmode";
	public static final String FULLTEXT = "fulltext";
	public static final String COL_FAMILY = "colfamily";
	public static final String COL_PREFIX = "colprefix";
	public static final String TABLE_KEYS = "keys";
	public static final String TABLE_KEY_ROW = "rowkey";
	public static final String TABLE_KEY_COL = "colkey";
	public static final String AGGR_SUB_KEY = "aggr_subkey";
	public static final String AGGR_SUB_GROUPBY = "aggr_subgroupby";
	public static final String TABLE_CONSTRUCT = "construct";
	public static final String TABLE_DESTRUCT = "destruct";
	public static final String TABLE_REFER = "refertbl";

	public <T> T attr(String attr) {
		return (T) attrs.get(attr);
	}

	public <T> T attr(String attr, Class<T> required) {
		return (T) attrs.get(attr);
	}

	public <T> T attr(String attr, T def) {
		return (T) attrs.getOrDefault(attr, def);
	}

	public <T> D attw(String attr, T value) {
		if (null != value) attrs.put(attr, value);
		else attrs.remove(attr);
		return (D) this;
	}

	public D attw(Map<String, ?> attrs) {
		this.attrs.putAll(attrs);
		return (D) this;
	}
}
