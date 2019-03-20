package net.butfly.albatis.ddl;

import static net.butfly.albacore.utils.Texts.eq;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;

import java.io.Serializable;
import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.vals.ValType;

public interface Builder {
	static FieldDesc field(TableDesc table, String fieldname, Map<String, ?> ops) {
		Object val;
		return new FieldDesc(table, fieldname, Qualifier.type(ops), (null != (val = ops.remove("rowkey")) ? Boolean.parseBoolean(val
				.toString()) : false)).attw(ops);
	}

	static FieldDesc field(TableDesc table, String fieldname, String desc) {
		Map<String, String> ops = Maps.ofQueryString(desc, "type");
		Object val;
		return new FieldDesc(table, fieldname, Qualifier.type(ops), (null != (val = ops.remove("rowkey")) ? Boolean.parseBoolean(val
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

	/**
	 * @author zbutf
	 *
	 */
	public static class Qualifier implements Serializable {
		private static final long serialVersionUID = 501714117033564122L;
		public final String table;
		public final String family;
		public final String prefix;
		public final String column;

		public final String qualifierTable;
		public final String columnQualifier;

		/**
		 * @param q
		 *            table:cf#prefix, cf:prefix#col<br>
		 * @return [table_name, col_family, col_prefix, col_name]
		 * @return
		 */
		public static Qualifier parse(String tqf, String fqf) {
			return new Qualifier(tqf, fqf);
		}

		public static Qualifier qf(String table, String family, String prefix, String column) {
			return new Qualifier(table, family, prefix, column);
		}

		private String qualiferTable() {
			if (null == table) return null;
			StringBuilder s = new StringBuilder(table);
			if (null != family) s.append(SPLIT_CF_CH + family);
			if (null != prefix) s.append(SPLIT_PREFIX_CH + prefix);
			return s.toString();
		}

		private String qualiferColumn() {
			if (null == column) return null;
			StringBuilder s = new StringBuilder();
			if (null != family) s.append(family + SPLIT_CF_CH);
			if (null != prefix) s.append(prefix + SPLIT_PREFIX_CH);
			s.append(column);
			return s.toString();
		}

		@Override
		public String toString() {
			if (null == table) return qualiferColumn();
			else if (null == column) return qualiferTable();
			else return qualiferTable() + "." + qualiferColumn();
		}

		protected Qualifier() {
			table = null;
			family = null;
			prefix = null;
			column = null;
			qualifierTable = null;
			columnQualifier = null;
		}

		public Qualifier(String tqf, String fqf) {
			String[] tqs = null == tqf ? new String[3] : parseTableName(tqf);
			String[] fqs = null == fqf ? new String[3] : parseFieldName(fqf);
			table = tqs[0];
			family = one(tqs[1], fqs[0]);
			prefix = one(tqs[2], fqs[1]);
			column = fqs[2];
			qualifierTable = qualiferTable();
			columnQualifier = qualiferColumn();
		}

		public Qualifier(String table, String family, String prefix, String column) {
			this.table = table;
			this.family = family;
			this.prefix = prefix;
			this.column = column;
			qualifierTable = qualiferTable();
			columnQualifier = qualiferColumn();
		}

		private static String one(String t, String f) {
			// return null == t ? f : t.equals(f) ? t : null;
			return null != f ? f : t;
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
				fqs[1] = fqs[2].substring(0, sp);
				fqs[2] = fqs[2].substring(sp + 1);
			}
			return fqs;
		}

		private static ValType type(Map<String, ?> ops) {
			ValType t = ValType.of(ops.remove("type").toString());
			Object val = ops.remove("length");
			Number len;
			if (null != val && val instanceof Number && (len = (Number) val).intValue() > 0) //
				t = t.len(len.longValue());
			return t;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) return true;
			if (null == obj || !(obj instanceof Qualifier)) return false;
			Qualifier q = (Qualifier) obj;
			return eq(column, q.column) && eq(prefix, q.prefix) && eq(family, q.family) && eq(table, q.table);
		}

		public Qualifier retable(String table) {
			return new Qualifier(table, family, prefix, column);
		}

		public Qualifier refield(String qf) {
			Qualifier q = parse(null, qf);
			return new Qualifier(table, one(family, q.family), one(prefix, q.prefix), q.column);
		}
	}
}
