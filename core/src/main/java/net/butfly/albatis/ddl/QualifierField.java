package net.butfly.albatis.ddl;

import static net.butfly.albacore.utils.Texts.eq;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_CF_CH;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX_CH;

public class QualifierField extends Qualifier {
	private static final long serialVersionUID = 5725397514982733160L;
	public final String field;
	public final String fieldPrefix;
	public final String fieldQualifier;

	QualifierField(String table, String family, String prefix, String field) {
		super(table, family, field);
		this.field = field;
		this.fieldPrefix = (null == family ? "" : family + SPLIT_CF_CH) + (null == prefix ? "" : prefix + SPLIT_PREFIX_CH);
		this.fieldQualifier = fieldPrefix + (null == name ? "" : name);
	}

	public Qualifier table() {
		return new Qualifier(name, family, prefix);
	}

	public QualifierField family(boolean enable) {
		return enable ? this : new QualifierField(name, null, prefix, field);
	}

	public QualifierField prefix(boolean enable) {
		return enable ? this : new QualifierField(name, family, null, (null == prefix ? "" : prefix + SPLIT_PREFIX_CH) + field);
	}

	@Override
	public String toString() {
		return fieldQualifier;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (null == obj || !(obj instanceof QualifierField)) return false;
		QualifierField q = (QualifierField) obj;
		return eq(name, q.name) && eq(prefix, q.prefix) && eq(family, q.family) && eq(name, q.name);
	}

	@Override
	public int hashCode() {
		return fieldQualifier.hashCode();
	}

}
