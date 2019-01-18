package net.butfly.albatis.kafka.avro;

import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.JSON_STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STRL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.avro.AvroSerDes;

@SerDes.As("avro")
public class AvroFormat extends RmapFormat {
	private static final long serialVersionUID = 3687957634350865452L;
	private static final Logger logger = Logger.getLogger(AvroFormat.class);
	private static final Map<String, Schema> SCHEMAS = Maps.of();
	private static final SchemaReg reg = new SchemaReg();

	@Override
	public Rmap ser(Rmap m, TableDesc dst) {
		return empty(dst.fields()) ? ser(m) : ser(m, SCHEMAS.computeIfAbsent(dst.name, t -> Builder.schema(dst)));
	}

	@Override
	public Rmap ser(Rmap m) {
		return empty(m) ? null : ser(m, AvroSerDes.Builder.schema(m));
	}

	protected Rmap ser(Rmap m, Schema schema) {
		byte[] b = reg.ser(m.table(), m.map(), schema);
		if (null == b || b.length == 0) return null;
		Rmap r = m.skeleton();
		r.put(UUID.randomUUID().toString(), b);
		return r;
	}

	@Override
	public Rmap deser(Rmap m, TableDesc dst) {
		if (empty(m)) return null;
		if (empty(dst.fields())) return deser(m);
		if (m.size() > 1) logger().warn("Multiple value deser as avro no supported, only first proc and other lost.");
		return deser(m, SCHEMAS.computeIfAbsent(dst.name, k -> Builder.schema(dst)));
	}

	protected Rmap deser(Rmap m, Schema schema) {
		byte[] v = (byte[]) m.entrySet().iterator().next().getValue();
		if (null == v || v.length == 0) return null;
		Map<String, Object> rm = reg.deser(m.table(), v, schema);
		if (empty(rm)) return null;
		Rmap r = m.skeleton();
		r.putAll(rm);
		return r;
	}

	@Override
	public Rmap deser(Rmap m) {
		if (empty(m)) return null;
		throw new UnsupportedOperationException("Deserialization as avro without schema (FieldDesc) is not supported.");
		// Schema schema = schema(m);
		// DatumReader<GenericRecord> r = new GenericDatumReader<GenericRecord>(schema);
		// return deserBySchema(m, r);
	}

	@Override
	public Rmap sers(List<Rmap> l, TableDesc dst) {
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public List<Rmap> desers(Rmap rmap, TableDesc dst) {
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public Class<?> formatClass() {
		return byte[].class;
	}

	public static class Builder {
		public static TableDesc schema(String table, Schema schema) {
			TableDesc t = new TableDesc(null, table);
			for (Schema.Field f : schema.getFields())
				t.field(field(t, f));
			return t;
		}

		public static Schema schema(TableDesc t) {
			Schema s = Schema.createRecord(t.name, null, null, false);
			s.setFields(Colls.list(Builder::field, t.fields()));
			logger.info("Schema build from field list: \n\t" + s.toString());
			return s;
		}

		@SuppressWarnings("deprecation")
		public static FieldDesc field(TableDesc t, Schema.Field f) {
			switch (f.schema().getType()) {
			case STRING:
				return new FieldDesc(t, f.name(), ValType.STR);
			case BYTES:
				return new FieldDesc(t, f.name(), ValType.BIN);
			case INT:
				return new FieldDesc(t, f.name(), ValType.INT);
			case LONG:
				return new FieldDesc(t, f.name(), ValType.LONG);
			case FLOAT:
				return new FieldDesc(t, f.name(), ValType.FLOAT);
			case DOUBLE:
				return new FieldDesc(t, f.name(), ValType.DOUBLE);
			case BOOLEAN:
				return new FieldDesc(t, f.name(), ValType.BOOL);
			case NULL:
				return new FieldDesc(t, f.name(), ValType.VOID);
			case RECORD:
			case ARRAY:
			case MAP:
			case UNION:
			case ENUM:
			case FIXED:
				return null;
			}
			return null;
		}

		public static Schema.Field field(FieldDesc f) {
			if (null == f) return null;
			switch (f.type.flag) {
			case BINARY:
				return new Schema.Field(f.name, Schema.create(Schema.Type.BYTES), null, new byte[0]);
			case INT:
				return new Schema.Field(f.name, Schema.create(Schema.Type.INT), null, 0);
			case DATE:
			case LONG:
				return new Schema.Field(f.name, Schema.create(Schema.Type.LONG), null, 0);
			case FLOAT:
				return new Schema.Field(f.name, Schema.create(Schema.Type.FLOAT), null, 0.0);
			case DOUBLE:
				return new Schema.Field(f.name, Schema.create(Schema.Type.DOUBLE), null, 0.0);
			case BOOL:
				return new Schema.Field(f.name, Schema.create(Schema.Type.BOOLEAN), null, false);
			case CHAR:
			case STR:
			case STRL:
			case GEO:
			case JSON_STR:
			case UNKNOWN:
				return new Schema.Field(f.name, Schema.create(Schema.Type.STRING), null, "");
			}
			return null;
		}
	}
}
