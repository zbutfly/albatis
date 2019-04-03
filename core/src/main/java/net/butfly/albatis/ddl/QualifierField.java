package net.butfly.albatis.ddl;

import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;

public class QualifierField extends Qualifier {
	private static final long serialVersionUID = 5725397514982733160L;
	public final Qualifier table;

	private QualifierField(Qualifier table, String family, String prefix, String field) {
		super(field, null == table.family ? null : family, null == table.prefix ? null : prefix);
		this.table = table;
	}

	@Override
	protected String qualify() {
		return null == name ? sub() : sub() + name;
	}

	@Override
	public String sub() {
		return (null == family ? "" : family + SPLIT_CF_CH) + (null == prefix ? "" : prefix + SPLIT_PREFIX_CH);
	}

	/**
	 * @param field
	 *            cf:prefix#col
	 * @return [(table_name, col_family, col_prefix), col_name]
	 * @return
	 */
	public static QualifierField of(Qualifier table, String field) {
		String[] fqs = parseFieldName(field);
		table = new Qualifier(table.name, one(table.family, fqs[0]), one(table.prefix, fqs[1]));
		return new QualifierField(table, table.family, table.prefix, fqs[2]);
	}

	public static QualifierField of(Qualifier table, String family, String prefix, String field) {
		table = new Qualifier(table.name, one(table.family, family), one(table.prefix, prefix));
		return new QualifierField(table, table.family, table.prefix, field);
	}

	public static QualifierField of(String table, String family, String prefix, String field) {
		return of(new Qualifier(table), family, prefix, field);
	}

	@Override
	public String toString() {
		return table.toString() + '.' + qualifier;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (null == obj || !(obj instanceof QualifierField)) return false;
		QualifierField q = (QualifierField) obj;
		return qualifier.equals(q.qualifier) && table.equals(q.table)/* same table */;
	}

	@Override
	public int hashCode() {
		return qualifier.hashCode();
	}

	public QualifierField table(Qualifier table) {
		String p = prefix;
		String f = name;
		if (null == table.prefix) {
			f = (null == prefix ? "" : prefix + SPLIT_PREFIX_CH) + f;
			p = null;
		}
		return new QualifierField(table, null == table.family ? null : family, p, f);
	}
}
