package net.butfly.albatis.ddl;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.vals.ValType;

/**
 * @author zx
 *
 */
public final class FieldDesc {
	public final String name;
	/**
	 * cf:prefix#name
	 */
	public final String fullname;
	public final ValType type;
	public final boolean rowkey;
	public final boolean unique;
	public final boolean nullable;
	// extra
	private final Map<String, Object> attrs = Maps.of();

	public FieldDesc(String name, ValType type, boolean rowkey, boolean unique, boolean nullable) {
		super();
		String[] cfPrefixName = parse(fullname);
		this.name = cfPrefixName[2];
		this.fullname = fullname;
		this.type = type;
		this.rowkey = rowkey;
		this.unique = unique;
		this.nullable = rowkey ? false : nullable;
	}

	public FieldDesc(String name, ValType type, boolean rowkey) {
		this(name, type, rowkey, false, !rowkey);
	}

	public FieldDesc(String fullname, ValType type) {
		this(fullname, type, false);
	}

	@Override
	public String toString() {
		return name + "[" + type.toString() + (rowkey ? ",KEY" : "") + "]";
	}

	public <T> FieldDesc attr(String attr, T value) {
		if (null != value) attrs.put(attr, value);
		else attrs.remove(attr);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <T> T attr(String attr) {
		return (T) attrs.get(attr);
	}

	public FieldDesc attr(Map<String, ?> attrs) {
		this.attrs.putAll(attrs);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <T> T attrTyp(String attr, Class<T> required) {
		return (T) attrs.get(attr);
	}

	@SuppressWarnings("unchecked")
	public <T> T attrDef(String attr, T def) {
		return (T) attrs.getOrDefault(attr, def);
	}

	protected static String[] parse(String qf) {
		String[] ss = new String[3];
		String[] s = qf.split(":");
		ss[0] = s.length > 1 ? s[0] : null;
		s = s[s.length - 1].split("#");
		ss[1] = s.length == 1 ? null : s[0];
		ss[2] = s[s.length - 1];
		return ss;
	}
}
