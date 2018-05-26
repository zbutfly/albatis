package net.butfly.albatis.ddl.vals;

import java.sql.Types;
import java.util.List;

import net.butfly.albacore.utils.collection.Colls;

public final class ListValType extends ValType {
	public final List<? extends ValType> types;

	ListValType(String[] flag, ValType... types) {
		super(List.class, List.class, Types.OTHER, flag);
		this.types = Colls.list(types);
	}

	public ListValType(ValType... types) {
		super(List.class, List.class, Types.OTHER);
		this.types = Colls.list(types);
	}

	public ListValType(Iterable<ValType> types) {
		super(List.class, List.class, Types.OTHER);
		this.types = Colls.list(types);
	}
}
