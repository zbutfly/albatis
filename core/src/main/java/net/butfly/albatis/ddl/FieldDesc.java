package net.butfly.albatis.ddl;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.vals.ValType;

/**
 * @author zx
 *
 */
public class FieldDesc {
	/**
	 * qualifier, maybe include cf/prefix and so on.
	 */
	public final String name;
	public final ValType type;
	public final boolean rowkey;
	public final boolean unique;
	public final boolean nullable;
	// extra
	private final Map<String, Object> attrs = Maps.of();

	public FieldDesc(String name, ValType type, boolean rowkey, boolean unique, boolean nullable) {
		super();
		this.name = name;
		this.type = type;
		this.rowkey = rowkey;
		this.unique = unique;
		this.nullable = rowkey ? false : nullable;
	}

	public FieldDesc(String name, ValType type, boolean rowkey) {
		this(name, type, rowkey, false, !rowkey);
	}

	public FieldDesc(String name, ValType type) {
		this(name, type, false);
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

	@SuppressWarnings("unchecked")
	public <T> T attrTyp(String attr, Class<T> required) {
		return (T) attrs.get(attr);
	}

	@SuppressWarnings("unchecked")
	public <T> T attrDef(String attr, T def) {
		return (T) attrs.getOrDefault(attr, def);
	}

}
