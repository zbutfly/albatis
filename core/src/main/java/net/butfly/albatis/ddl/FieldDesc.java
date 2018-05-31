package net.butfly.albatis.ddl;

import net.butfly.albatis.ddl.vals.ValType;

public final class FieldDesc extends Desc<FieldDesc> {
	public final String name;
	/**
	 * cf:prefix#name
	 */
	public final String fullname;
	public final ValType type;
	public final boolean rowkey;
	public final boolean unique;
	public final boolean nullable;

	public FieldDesc(String fullname, ValType type, boolean rowkey, boolean unique, boolean nullable) {
		super();
		this.name = parse(fullname);
		this.fullname = fullname;
		this.type = type;
		this.rowkey = rowkey;
		this.unique = unique;
		this.nullable = rowkey ? false : nullable;
	}

	public FieldDesc(String fullname, ValType type, boolean rowkey) {
		this(fullname, type, rowkey, false, !rowkey);
	}

	public FieldDesc(String fullname, ValType type) {
		this(fullname, type, false);
	}

	@Override
	public String toString() {
		return name + "[" + type.toString() + (rowkey ? ",KEY" : "") + "]";
	}

	private String parse(String qf) {
		String cf = null, prefix = null, name;
		String[] s = qf.split("#", 2);
		if (s.length > 1) {
			name = s[1];
			s = s[0].split(":", 2);
			if (s.length > 1) {
				cf = s[0];
				prefix = s[1];
			}
		} else name = s[0];
		attw(Desc.COL_FAMILY, cf).attw(Desc.COL_PREFIX, prefix);
		return name;
	}
}
