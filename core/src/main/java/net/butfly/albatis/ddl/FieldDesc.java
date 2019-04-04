package net.butfly.albatis.ddl;

import static net.butfly.albatis.ddl.DBDesc.logger;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.vals.ValType;

public final class FieldDesc extends Desc<FieldDesc> {
	private static final long serialVersionUID = -7563169977573257870L;
	public static final char SPLIT_CF_CH = ':';
	public static final String SPLIT_CF = ":";
	public static final char SPLIT_PREFIX_CH = '#';
	public static final String SPLIT_PREFIX = "#";
	public static final char SPLIT_ZWNJ_CH = (char) 0x200C;
	public static final String SPLIT_ZWNJ = Character.toString(SPLIT_ZWNJ_CH);
	/**
	 * cf:prefix#name
	 */
	public final Qualifier qualifier;
	public final ValType type;
	public final boolean rowkey;
	public final boolean unique;
	public final boolean nullable;

	public FieldDesc(TableDesc table, String qualifier, ValType type, boolean rowkey, boolean unique, boolean nullable) {
		super();
		this.qualifier = table.qualifier.refield(qualifier);
		this.type = type;
		this.rowkey = rowkey;
		this.unique = unique;
		this.nullable = rowkey ? false : nullable;
		if (null != table) table.field(this);
	}


	public FieldDesc(TableDesc table, String qualifier, ValType type, boolean rowkey) {
		this(table, qualifier, type, rowkey, false, rowkey);
	}

	public FieldDesc(TableDesc table, String qualifier, ValType type) {
		this(table, qualifier, type, false);
	}

	@Override
	public String toString() {
		return "Field[" + qualifier + "](" + type.toString() + (rowkey ? ",KEY" : "") + ")" + super.toString();
	}
}
