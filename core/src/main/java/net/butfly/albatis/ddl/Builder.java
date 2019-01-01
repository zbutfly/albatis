package net.butfly.albatis.ddl;

import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.vals.ValType;

public abstract class Builder {
	public static FieldDesc field(TableDesc table, String fieldname, Map<String, ?> ops) {
		Object val;
		return new FieldDesc(table, fieldname, type(ops), (null != (val = ops.remove("rowkey")) ? Boolean.parseBoolean(val.toString())
				: false)).attw(ops);
	}

	public static FieldDesc field(TableDesc table, String fieldname, String desc) {
		Map<String, String> ops = Maps.ofQueryString(desc, "type");
		Object val;
		return new FieldDesc(table, fieldname, type(ops), (null != (val = ops.remove("rowkey")) ? Boolean.parseBoolean(val.toString())
				: false)).attw(ops);
	}

	@Deprecated
	public static FieldDesc field(TableDesc table, String fullname, ValType type, boolean rowkey, String format, String validExpr,
			int segmode, String... copyto) {
		return new FieldDesc(table, fullname, type, rowkey).attw(Desc.FORMAT, format).attw(Desc.VALIDATE, expr(validExpr))//
				.attw(Desc.SEGMODE, segmode).attw(Desc.FULLTEXT, copyto);
	}

	private static ValType type(Map<String, ?> ops) {
		ValType t = ValType.of(ops.remove("type").toString());
		Object val = ops.remove("length");
		Number len;
		if (null != val && val instanceof Number && (len = (Number) val).intValue() > 0) //
			t = t.len(len.longValue());
		return t;
	}

	public static String expr(String expr) {
		return null == expr ? null : expr.charAt(0) == '=' ? expr.substring(1) : expr;
	}

	/**
	 * @return [table_name, col_family, col_prefix, col_name]
	 */
	public static String[] parseTableAndField(String tsub, String fq) {
		String[] tqs = parseTableName(tsub);
		String[] fqs = parseFieldName(fq);
		return new String[] { tqs[0], one(fqs[0], tqs[1]), one(fqs[1], tqs[2]), fqs[2] };
	}

	private static String one(String s1, String s2) {
		// return null == s1 ? s2 : s1.equals(s2) ? s1 : null;
		return null != s2 ? s2 : s1;
	}

	// table#cf:prefix
	private static String[] parseTableName(String tsub) {
		String[] fqs = new String[3];
		if (null == tsub) return fqs;
		fqs[0] = tsub;
		String[] cfs = fqs[0].split(SPLIT_CF, 2); // cf and prefix#colkey
		if (cfs.length == 2) {
			fqs[0] = cfs[0];
			fqs[1] = cfs[1];
		}
		if (null != fqs[1]) {
			int sp = fqs[1].lastIndexOf(SPLIT_PREFIX_CH);
			if (sp >= 0) {
				fqs[2] = fqs[1].substring(sp + 1);
				fqs[1] = fqs[1].substring(0, sp);
			}
		}
		return fqs;
	}

	// cf#prefix:colkey
	private static String[] parseFieldName(String fq) {
		String[] fqs = new String[3];
		if (null == fq) return fqs;
		fqs[2] = fq;
		String[] cfs = fqs[2].split(SPLIT_CF, 2); // cf and prefix#colkey
		if (cfs.length == 2) {
			fqs[0] = cfs[0];
			fqs[2] = cfs[1];
		}
		int sp = fqs[2].lastIndexOf(SPLIT_PREFIX_CH);
		if (sp >= 0) {
			fqs[2] = fqs[2].substring(sp + 1);
			fqs[1] = fqs[1].substring(0, sp);
		}
		return fqs;
	}
}
