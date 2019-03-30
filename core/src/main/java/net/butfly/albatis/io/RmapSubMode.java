package net.butfly.albatis.io;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.QualifierField;

public enum RmapSubMode {
	NONE(0), FULL(RmapSubMode.PREFIX_ENABLED | RmapSubMode.FAMILY_ENABLED), //
	FAMILY_ONLY(RmapSubMode.FAMILY_ENABLED), PREFIX_ONLY(RmapSubMode.PREFIX_ENABLED);
	public static final int FAMILY_ENABLED = 0x1;
	public static final int PREFIX_ENABLED = 0x2;
	private final int flag;

	private RmapSubMode(int flag) {
		this.flag = flag;
	}

	public boolean splitBy(int flag) {
		return (flag & this.flag) > 0;
	}

	public QualifierField proc(Qualifier table, String field) {
		return new QualifierField(table, field) //
				.family(!splitBy(RmapSubMode.FAMILY_ENABLED)) // if not split by cf, remove the cf from qualifier
				.prefix(!splitBy(RmapSubMode.PREFIX_ENABLED)); // if not split by prefix, merge the prefix into field name of qualifier
	}
}