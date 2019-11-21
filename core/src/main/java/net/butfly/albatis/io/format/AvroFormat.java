package net.butfly.albatis.io.format;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.avro.AvroSerDes;

@SerDes.As("avro")
public class AvroFormat extends RmapFormat {
	private static final long serialVersionUID = 3687957634350865452L;
	private static final Logger logger = Logger.getLogger(AvroFormat.class);
	private static final Map<Qualifier, Schema> SCHEMAS = Maps.of();
	private static final AvroSerdes reg = AvroSerdes.init();

	@Override
	public Rmap ser(Rmap m, TableDesc dst) {
		return empty(dst.fields()) ? ser(m) : ser(m, SCHEMAS.computeIfAbsent(dst.qualifier, t -> Builder.schema(dst)));
	}

	@Override
	public Rmap ser(Rmap m) {
		return empty(m) ? null : ser(m, AvroSerDes.Builder.schema(m));
	}

	protected Rmap ser(Rmap m, Schema schema) {
		byte[] b = reg.ser(m.table().name, m.map(), schema);
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
		return deser(m, SCHEMAS.computeIfAbsent(dst.qualifier, k -> Builder.schema(dst)));
	}

	protected Rmap deser(Rmap m, Schema schema) {
		byte[] v = (byte[]) m.entrySet().iterator().next().getValue();
		if (null == v || v.length == 0) return null;
		Map<String, Object> rm = reg.deser(m.table().name, v, schema);
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

	public static interface Builder {
		static TableDesc schema(String table, Schema schema) {
			TableDesc t = TableDesc.dummy(table);
			for (Schema.Field f : schema.getFields()) t.field(field(t, f));
			return t;
		}

		static Schema schema(TableDesc t) {
			Map<String, Object> m = Maps.of("DISP_NAME", t.attr("dispName"), "COMMENT", t.attr("comment"));
			String doc = Colls.empty(m) ? null : JsonSerder.JSON_MAPPER.ser(m);
			Schema s = Schema.createRecord(t.qualifier.name, doc, null, false);
			s.setFields(Colls.list(Builder::field, t.fields()));
			logger.info("Schema build from field list: \n\t" + s.toString());
			return s;
		}

		@SuppressWarnings("deprecation")
		static FieldDesc field(TableDesc table, Schema.Field f) {
			Schema.Type t;
			try {
				t = restrictType(f);
			} catch (IllegalArgumentException e) {
				return null;
			}
			switch (t) {
			case STRING:
				return new FieldDesc(table, f.name(), ValType.STR);
			case BYTES:
				return new FieldDesc(table, f.name(), ValType.BIN);
			case INT:
				return new FieldDesc(table, f.name(), ValType.INT);
			case LONG:
				return new FieldDesc(table, f.name(), ValType.LONG);
			case FLOAT:
				return new FieldDesc(table, f.name(), ValType.FLOAT);
			case DOUBLE:
				return new FieldDesc(table, f.name(), ValType.DOUBLE);
			case BOOLEAN:
				return new FieldDesc(table, f.name(), ValType.BOOL);
			case NULL:
				return new FieldDesc(table, f.name(), ValType.VOID);
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

		static Schema.Type restrictType(Schema.Field f) {
			Schema.Type t = f.schema().getType();
			switch (t) {
			case RECORD:
			case ARRAY:
			case ENUM:
			case MAP:
			case FIXED:
			case NULL:
				throw new IllegalArgumentException(t.toString());
			case STRING:
			case BYTES:
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return t;
			case UNION:
				for (Schema ts : f.schema().getTypes()) {
					switch (ts.getType()) {
					case RECORD:
					case ARRAY:
					case ENUM:
					case MAP:
					case FIXED:
					case UNION:
						throw new IllegalArgumentException(ts.getType().toString());
					case NULL:
						continue;
					case STRING:
					case BYTES:
					case INT:
					case LONG:
					case FLOAT:
					case DOUBLE:
					case BOOLEAN:
						return ts.getType();
					}
				}
				break;
			}
			throw new IllegalArgumentException(f.toString());
		}

		static Schema.Field field(FieldDesc f) {
			if (null == f) return null;
			Map<String, Object> m = Maps.of("DISP_NAME", f.attr("dispName"), "COMMENT", f.attr("comment"));
			String doc = Colls.empty(m) ? null : JsonSerder.JSON_MAPPER.ser(m);
			switch (f.type.flag) {
			case BINARY:
				return new Schema.Field(f.name, createOptional(Schema.create(Schema.Type.BYTES)), doc, new byte[0]);
			case INT:
				return new Schema.Field(f.name, createOptional(Schema.create(Schema.Type.INT)), doc, 0);
			case LONG:
			case DATE:
				return new Schema.Field(f.name, createOptional(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))),
						doc, 0);
			case FLOAT:
				return new Schema.Field(f.name, createOptional(Schema.create(Schema.Type.FLOAT)), doc, 0.0);
			case DOUBLE:
				return new Schema.Field(f.name, createOptional(Schema.create(Schema.Type.DOUBLE)), doc, 0.0);
			case BOOL:
				return new Schema.Field(f.name, createOptional(Schema.create(Schema.Type.BOOLEAN)), doc, false);
			case CHAR:
			case STR:
			case STRL:
			case GEO:
			case JSON_STR:
			case UNKNOWN:
				return new Schema.Field(f.name, createOptional(Schema.create(Schema.Type.STRING)), doc, "");
			default:
				return null;
			}
		}

		static Schema createOptional(Schema schema) {
			return Schema.createUnion(Arrays.asList(schema, Schema.create(Schema.Type.NULL)));
		}
	}

	public interface AvroSerdes {
		byte[] ser(String topic, Map<String, Object> m, Schema schema);

		Map<String, Object> deser(String topic, byte[] v, Schema schema);

		static AvroSerdes init() {
			Set<Class<? extends AvroSerdes>> impl = Reflections.getSubClasses(AvroSerdes.class);
			if (impl.isEmpty()) return null;
			else {
				Class<? extends AvroSerdes> c = impl.iterator().next();
				logger.debug("Avro schema registry init as: " + c.getName());
				AvroSerdes r = Reflections.construct(c);
				if (null == r) logger.warn("Avro schema registry init faliure: " + c.getName());
				return r;
			}
		}
	}
}
