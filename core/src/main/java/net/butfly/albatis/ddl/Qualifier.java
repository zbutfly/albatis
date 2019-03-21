package net.butfly.albatis.ddl;

import static net.butfly.albacore.utils.Texts.eq;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;

import java.io.Serializable;

import net.butfly.albacore.utils.Pair;

public class Qualifier implements Serializable {
	private static final long serialVersionUID = 501714117033564122L;
	public final String table;
	public final String family;
	public final String prefix;

	public final String tableQualifier;

	protected Qualifier() {
		table = null;
		family = null;
		prefix = null;
		tableQualifier = null;
	}

	public Qualifier(String table, String family, String prefix) {
		this.table = table;
		this.family = family;
		this.prefix = prefix;
		tableQualifier = qualify();
	}

	/**
	 * @param q
	 *            table:cf#prefix
	 * @return (table_name, col_family, col_prefix)
	 * @return
	 */
	public static Qualifier qf(String tqf) {
		String[] tqs = parseTableName(tqf);
		return new Qualifier(tqs[0], tqs[1], tqs[2]);
	}

	public static Qualifier qf(String table, String family, String prefix) {
		return new Qualifier(table, family, prefix);
	}

	/**
	 * @param q
	 *            cf:prefix#col
	 * @return [(table_name, col_family, col_prefix), col_name]
	 * @return
	 */
	public Pair<Qualifier, String> colkey(String fqf) {
		String[] fqs = parseFieldName(fqf);
		return new Pair<>(new Qualifier(table, one(family, fqs[0]), one(prefix, fqs[1])), fqs[2]);
	}

	private String qualify() {
		if (null == table) return null;
		StringBuilder s = new StringBuilder(table);
		if (null != family) s.append(SPLIT_CF_CH + family);
		if (null != prefix) s.append(SPLIT_PREFIX_CH + prefix);
		return s.toString();
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

	public Qualifier table(String table) {
		return new Qualifier(table, family, prefix);
	}

	@Override
	public String toString() {
		if (null == table) return "[" + family + SPLIT_CF_CH + prefix + SPLIT_PREFIX_CH + "]";
		else return tableQualifier;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (null == obj || !(obj instanceof Qualifier)) return false;
		Qualifier q = (Qualifier) obj;
		return eq(prefix, q.prefix) && eq(family, q.family) && eq(table, q.table);
	}
}
