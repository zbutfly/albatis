package net.butfly.albatis.io;

import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;

public interface IOSchemaness extends Loggable {
	static final Map<IOSchemaness, Map<String, TableDesc>> SCHEMAS = Maps.of();

	enum SchemaMode {
		NONE, SINGLE, MULTIPLE
	}

	default SchemaMode schemaMode() {
		switch (schemaAll().size()) {
		case 0:
			return SchemaMode.NONE;
		case 1:
			return SchemaMode.SINGLE;
		default:
			return SchemaMode.MULTIPLE;
		}
	}

	default Map<String, TableDesc> schemaAll() {
		return SCHEMAS.computeIfAbsent(this, io -> Maps.of());
	}

	default TableDesc schema(String table) {
		return schemaAll().get(table);
	}

	default TableDesc schema(Qualifier table) {
		return schemaAll().get(table.toString());
	}

	@SuppressWarnings("unchecked")
	default <T extends IO> T schema(TableDesc... table) {
		if (0 == table.length) return (T) this;
		String s = table.length == 1 ? "Schema registered: " + table[0].toString()
				: "Schema registered: \n\t" + String.join("\n\t", Colls.list(TableDesc::toString, table));
		logger().info(s);
		for (TableDesc td : table) schemaAll().put(td.qualifier.name, td);
		return (T) this;
	}

	default boolean schemaExists(String table) {
		return schemaAll().containsKey(table);
	}
}
