package net.butfly.albatis.ddl.vals;

import java.sql.Types;
import java.util.Map;

public final class MapValType extends ValType {
	public final Map<String, ? extends ValType> mapTypes;

	public MapValType(Map<String, ? extends ValType> mapTypes) {
		super(Map.class, Map.class, Types.OTHER);
		this.mapTypes = mapTypes;
	}
}
