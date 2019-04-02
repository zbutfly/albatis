package net.butfly.albatis.ddl;

import static net.butfly.albacore.utils.Texts.eq;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;
import static net.butfly.albatis.ddl.Qualifier.one;

import java.io.Serializable;

public class QualifierField implements Serializable {
	private static final long serialVersionUID = 5725397514982733160L;
	public final Qualifier table;
	public final String name;
	public final String family;
	public final String prefix;
	public final String qualifier;

	/**
	 * @param field cf:prefix#col
	 * @return [(table_name, col_family, col_prefix), col_name]
	 * @return
	 */
	public QualifierField(Qualifier table, String field) {
		String[] fqs = parseFieldName(field);
		this.table = new Qualifier(table.name, one(table.family, fqs[0]), one(table.prefix, fqs[1]));
		this.name = fqs[2];

		this.family = null == table.family ? "" : table.family + SPLIT_CF_CH;
		this.prefix = null == table.prefix ? "" : table.prefix + SPLIT_PREFIX_CH;
		qualifier = null == name ? prefix : prefix + name;
	}

	public QualifierField(Qualifier table, String family, String prefix, String field) {
		this.table = new Qualifier(table.name, one(table.family, family), one(table.prefix, prefix));
		this.name = field;
		this.family = null == this.table.family ? "" : this.table.family + SPLIT_CF_CH;
		this.prefix = null == this.table.prefix ? "" : this.table.prefix + SPLIT_PREFIX_CH;
		qualifier = null == name ? prefix : prefix + name;
	}

	public QualifierField(String table, String family, String prefix, String field) {
		this.table = new Qualifier(table, family, prefix);
		this.name = field;
		this.family = null == this.table.family ? "" : this.table.family + SPLIT_CF_CH;
		this.prefix = null == this.table.prefix ? "" : this.table.prefix + SPLIT_PREFIX_CH;
		qualifier = null == name ? prefix : prefix + name;
	}

	public QualifierField family(boolean enable) {
		return enable ? this : new QualifierField(table, null, table.prefix, name);
	}

	public QualifierField prefix(boolean enable) {
		return enable ? this
				: new QualifierField(table, table.family, null,
						null == prefix ? name : prefix + SPLIT_PREFIX_CH + name);
	}

	@Override
	public String toString() {
		return qualifier;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (null == obj || !(obj instanceof QualifierField))
			return false;
		QualifierField q = (QualifierField) obj;
		return eq(name, q.name) && (table == q.table || (null != table && table.equals(q.table))/* same table */);
	}

	@Override
	public int hashCode() {
		return qualifier.hashCode();
	}

	// cf#prefix:colkey
	private static String[] parseFieldName(String fq) {
		String[] fqs = new String[3];
		if (null == fq)
			return fqs;
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
}
