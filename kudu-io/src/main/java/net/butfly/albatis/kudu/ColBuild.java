package net.butfly.albatis.kudu;

import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.TIMESTAMP;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.JSON_STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STRL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.apache.kudu.Type;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.FieldDesc;

interface ColBuild {
	static ColumnSchema buildColumn(FieldDesc f) {
		Type type = null;
		if (f.type == null) type = Type.STRING;
		else switch (f.type.flag) {
		case DATE:
			type = Type.INT64;
			break;
		case TIMESTAMP:
			type = Type.INT64;
			break;
		case INT:
		case LONG:
			type = Type.INT64;
			break;
		case FLOAT:
		case DOUBLE:
			type = Type.DOUBLE;
			break;
		case BOOL:
			type = Type.BOOL;
			break;
		case CHAR:
		case STR:
		case STRL:
		case GEO:
		case JSON_STR:
		case UNKNOWN:
			type = Type.STRING;
			break;
		case BINARY:
			type = Type.BINARY;
			break;
		}
		if (type == null) throw new RuntimeException("Kudu type not mapped on [" + f.type + "] on field mapping: " + f.toString());
		ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(f.name, type).encoding(type.equals(Type.STRING)
				? Encoding.DICT_ENCODING
				: Encoding.BIT_SHUFFLE).compressionAlgorithm(CompressionAlgorithm.LZ4).nullable(!f.rowkey).key(f.rowkey);
		return builder.build();
	}

	public static ColumnSchema[] buildColumns(boolean generateAutoKey, FieldDesc... fields) {
		ConcurrentMap<String, ColumnSchema> cols = Maps.of();
		for (FieldDesc f : fields)
			cols.putIfAbsent(f.name, buildColumn(f));
		List<ColumnSchema> l = new ArrayList<>(cols.values());
		l.sort((col1, col2) -> {
			if (col1.isKey() && col2.isKey()) return 0;
			if (col1.isKey()) return -1;
			if (col2.isKey()) return 1;
			return 0;
		});
		// Automatic Generated IIDD
		if (!l.get(0).isKey() && generateAutoKey) l.set(0, new ColumnSchema.ColumnSchemaBuilder("IIDD", Type.INT64).key(true).nullable(
				false).build());
		return l.toArray(new ColumnSchema[l.size()]);
	}
}
