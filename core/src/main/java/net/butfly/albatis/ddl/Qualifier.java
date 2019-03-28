package net.butfly.albatis.ddl;

import static net.butfly.albacore.utils.Texts.eq;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;

import java.io.Serializable;

public class Qualifier implements Serializable {
	private static final long serialVersionUID = 501714117033564122L;
	public final String name;
	public final String family;
	public final String prefix;

	public final String qualifier;

	protected Qualifier() {
		name = null;
		family = null;
		prefix = null;
		qualifier = null;
	}

	public Qualifier(String nameOrQf, String family, String prefix) {
		String[] tqs = parseTableName(nameOrQf);
		this.name = tqs[0];
		this.family = one(tqs[1], family);
		this.prefix = one(tqs[2], prefix);
		qualifier = qualify();
	}

	/**
	 * @param q
	 *            table:cf#prefix
	 * @return (table_name, col_family, col_prefix)
	 * @return
	 */
	public Qualifier(String tqf) {
		this(tqf, null, null);
	}

	private String qualify() {
		if (null == name) return null;
		StringBuilder s = new StringBuilder(name);
		if (null != family) s.append(SPLIT_CF_CH + family);
		if (null != prefix) s.append(SPLIT_PREFIX_CH + prefix);
		return s.toString();
	}

	static String one(String t, String f) {
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

	public Qualifier table(String table) {
		return new Qualifier(table, family, prefix);
	}

	@Override
	public String toString() {
		if (null == name) return "[" + family + SPLIT_CF_CH + prefix + SPLIT_PREFIX_CH + "]";
		else return qualifier;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (null == obj || !(obj instanceof Qualifier)) return false;
		Qualifier q = (Qualifier) obj;
		return eq(prefix, q.prefix) && eq(family, q.family) && eq(name, q.name);
	}

	@Override
	public int hashCode() {
		return qualifier.hashCode();
	}
}
