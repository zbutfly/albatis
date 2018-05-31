package net.butfly.albatis.ddl;

import net.butfly.albatis.ddl.vals.ValType;

/**
 * @author zx
 *
 */
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
	// extra

	public FieldDesc(String fullname, ValType type, boolean rowkey, boolean unique, boolean nullable) {
		super();
		String[] cfPrefixName = parse(fullname);
		this.name = cfPrefixName[2];
		this.fullname = fullname;
		this.type = type;
		this.rowkey = rowkey;
		this.unique = unique;
		this.nullable = rowkey ? false : nullable;
		attw(Desc.COL_FAMILY, cfPrefixName[0]).attw(Desc.COL_PREFIX, cfPrefixName[1]);
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
