package net.butfly.albatis.ddl;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.vals.ValType;

public interface Builder {
	static FieldDesc field(TableDesc table, String fieldname, Map<String, ?> ops) {
		Object val;
		return new FieldDesc(table, fieldname, type(ops), (null != (val = ops.remove("rowkey")) ? Boolean.parseBoolean(val
				.toString()) : false)).attw(ops);
	}

	static FieldDesc field(TableDesc table, String fieldname, String desc) {
		Map<String, String> ops = Maps.ofQueryString(desc, "type");
		Object val;
		return new FieldDesc(table, fieldname, type(ops), (null != (val = ops.remove("rowkey")) ? Boolean.parseBoolean(val
				.toString()) : false)).attw(ops);
	}

	@Deprecated
	static FieldDesc field(TableDesc table, String qualifier, ValType type, boolean rowkey, String format, String validExpr, int segmode,
			String... copyto) {
		return new FieldDesc(table, qualifier, type, rowkey).attw(Desc.FORMAT, format).attw(Desc.VALIDATE, expr(validExpr))//
				.attw(Desc.SEGMODE, segmode).attw(Desc.FULLTEXT, copyto);
	}

	static String expr(String expr) {
		return null == expr ? null : expr.charAt(0) == '=' ? expr.substring(1) : expr;
	}

	static ValType type(Map<String, ?> ops) {
		ValType t = ValType.of(ops.remove("type").toString());
		Object val = ops.remove("length");
		Number len;
		if (null != val && val instanceof Number && (len = (Number) val).intValue() > 0) //
			t = t.len(len.longValue());
		return t;
	}
}
