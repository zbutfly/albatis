package net.butfly.albatis.ddl;

import java.io.Serializable;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

@SuppressWarnings("unchecked")
public abstract class Desc<D extends Desc<D>> implements Serializable {
	private static final long serialVersionUID = -152619742670508854L;
	protected final Map<String, Object> attrs = Maps.of();
	public static final String DRY = "dry";
	// field desc
	public static final String FORMAT = "format";
	public static final String VALIDATE = "validate";
	public static final String SEGMODE = "segmode";
	public static final String FULLTEXT = "fulltext";
	public static final String INDEXED = "indexed";
	public static final String PROJECT_FROM = "asfrom";
	// field aggr
	public static final String AGGR_MODE = "aggr";
	public static final String AGGR_SUB_KEY = "aggr.sub.key";
	public static final String AGGR_SUB_GROUPBY = "aggr.sub.groupby";

	// table desc
	// public static final String COL_FAMILY = "colfamily";
	// public static final String COL_PREFIX = "colprefix";
	public static final String TABLE_KEY_ROW = "rowkey";
	public static final String TABLE_KEY_COL = "colkey";
	public static final String TABLE_QUERYPARAM = "queryparam";
	public static final String TABLE_QUERYPARAM_FIELD = "table_queryparam_field";
	// public static final String TABLE_KEYS = "keys";
	// public static final String TABLE_CONSTRUCT = "construct";
	// public static final String TABLE_DESTRUCT = "destruct";
	// public static final String TABLE_REFER = "refertbl";

	// for debug, dpc db config info
	public static final String DPC_ID = "dpc.id";
	public static final String DPC_NAME = "dpc.name";

	public <T> T attr(String attr) {
		return (T) attrs.get(attr);
	}

	public <T> T attrm(String attr) {
		return (T) attrs.remove(attr);
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
		if (!Colls.empty(attrs)) this.attrs.putAll(attrs);
		return (D) this;
	}

	public D attw(Desc<D> attrs) {
		return attw(attrs.attrs);
	}

	@Override
	public String toString() {
		return attrs.isEmpty() ? "" : (" (" + attrs.toString() + ")");
	}
}
