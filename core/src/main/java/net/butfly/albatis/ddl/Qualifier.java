package net.butfly.albatis.ddl;

import static net.butfly.albacore.utils.Texts.eq;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;

import java.io.Serializable;

public class Qualifier implements Serializable {
	private static final long serialVersionUID = 501714117033564122L;
	public final String name;
	public final String family;
	public final String prefix;
	public final String qualifier;

	protected Qualifier(String... nameFamilyPrefix) {
		this.name = nameFamilyPrefix[0];
		this.family = nameFamilyPrefix.length > 1 ? nameFamilyPrefix[1] : null;
		this.prefix = nameFamilyPrefix.length > 2 ? nameFamilyPrefix[2] : null;
		this.qualifier = qualify();
	}

	public Qualifier(String nameOrQf, String family, String prefix) {
		this(parseTableName(nameOrQf, family, prefix));
	}

	protected String qualify() {
		return null == name ? sub() : name + sub();
	}

	public String sub() {
		return (null == family ? "" : SPLIT_CF_CH + family) + (null == prefix ? "" : SPLIT_PREFIX_CH + prefix);
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

	public Qualifier table(String table) {
		return new Qualifier(table, family, prefix);
	}

	public Qualifier family(String family) {
		return new Qualifier(name, family, prefix);
	}

	public Qualifier prefix(String prefix) {
		return new Qualifier(name, family, prefix);
	}

	@Override
	public String toString() {
		return qualifier;
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

	//
	// cf#prefix:colkey
	static String[] parseFieldName(String fq) {
		String[] fqs = new String[3];
		if (null == fq) return fqs;
		fqs[2] = fq;
		String[] cfs = fqs[2].split(SPLIT_CF, 2); // cf and prefix#colkey
		if (cfs.length == 2) {
			fqs[0] = cfs[0];
			fqs[2] = cfs[1];
		}
		String[] pfxs = fqs[2].split(SPLIT_PREFIX, 2);
		if (pfxs.length == 2) {
			fqs[1] = pfxs[0];
			fqs[2] = pfxs[1];
		}
		return fqs;
	}

	// table#cf:prefix
	static String[] parseTableName(String table, String family, String prefix) {
		String[] fqs = new String[3];
		if (null == table) return fqs;
		fqs[0] = table;
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
		return new String[] { fqs[0], one(fqs[1], family), one(fqs[2], prefix) };

	}

	static String one(String t, String f) {
		// return null == t ? f : t.equals(f) ? t : null;
		return null != f ? f : t;
	}

}
