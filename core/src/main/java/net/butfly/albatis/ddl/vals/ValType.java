package net.butfly.albatis.ddl.vals;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;

public abstract class ValType implements Serializable {
	private static final long serialVersionUID = 1262373128170249323L;
	protected final static Map<String, ValType> MAPPING = Maps.of();

	public static interface Flags {
		@Deprecated
		public static final String VOID = "v";
		// @Deprecated
		public static final String UNKNOWN = "";
		// basic type: primitive
		public static final String BOOL = "bl";
		public static final String CHAR = "c";
		public static final String BYTE = "b";
		public static final String SHORT = "sh";
		public static final String INT = "i";
		public static final String LONG = "l";
		public static final String FLOAT = "f";
		public static final String DOUBLE = "d";
		// basic type: extended
		public static final String STR = "s";
		public static final String STRL = "strl";
		public static final String BINARY = "bin";
		public static final String DATE = "dt";
		public static final String TIMESTAMP = "ts";
		// assembly type
		public static final String GEO = "g";
		public static final String LOCATION_RPT = "srpt";
		public static final String JSON_STR = "json_str";
		public static final String JSONARRAY_STR="jsonarray_str";
		public static final String GEO_PG_GEOMETRY ="geo_pg_geometry";
		public static final String GEO_SHAPE="geo_shape";
	}

	@Deprecated
	public static final BasicValType VOID = new BasicValType(void.class, Void.class, Types.OTHER, Flags.VOID, "void");
	// @Deprecated
	public static final BasicValType UNKNOWN = new BasicValType(Object.class, Object.class, Types.OTHER, Flags.UNKNOWN);
	// basic type: primitive
	public static final BasicValType BOOL = new BasicValType(boolean.class, Boolean.class, Types.BOOLEAN, Flags.BOOL, "bool", "boolean");
	public static final BasicValType CHAR = new BasicValType(char.class, Character.class, Types.CHAR, Flags.CHAR, "char");
	public static final BasicValType BYTE = new BasicValType(byte.class, Byte.class, Types.TINYINT, Flags.BYTE, "byte", "tinyint");
	public static final BasicValType SHORT = new BasicValType(short.class, Short.class, Types.SMALLINT, Flags.SHORT, "short", "smallint");
	public static final BasicValType INT = new BasicValType(int.class, Integer.class, Types.INTEGER, Flags.INT, "int", "integer");
	public static final BasicValType LONG = new BasicValType(long.class, Long.class, Types.BIGINT, Flags.LONG, "long", "bigint");
	public static final BasicValType FLOAT = new BasicValType(float.class, Float.class, Types.FLOAT, Flags.FLOAT, "float", "decimal");
	public static final BasicValType DOUBLE = new BasicValType(double.class, Double.class, Types.DOUBLE, Flags.DOUBLE, "double",
			"bigdecimal");
	// basic type: extended
	public static final BasicValType STR = new BasicValType(CharSequence.class, CharSequence.class, Types.NVARCHAR, Flags.STR, "str",
			"string");
	public static final BasicValType STRL = new BasicValType(CharSequence.class, CharSequence.class, Types.LONGNVARCHAR, Flags.STRL,
			"str_long");
	public static final BasicValType BIN = new BasicValType(byte[].class, ByteBuffer.class, Types.BINARY, Flags.BINARY, "binary");
	public static final BasicValType DATE = new BasicValType(Date.class, Date.class, Types.TIMESTAMP, Flags.DATE, "date", "time",
			"datetime");
	public static final BasicValType TIMESTAMP = new BasicValType(Timestamp.class, Timestamp.class, Types.TIMESTAMP, Flags.TIMESTAMP,"timestamp");
	// assembly type
	public static final ListValType GEO = new ListValType(new String[] { Flags.GEO, "geo" }, DOUBLE, DOUBLE);
	public static final ListValType LOCATION_RPT = new ListValType(new String[] { Flags.LOCATION_RPT, "string_srpt" }, STR);
	public static final ListValType JSON_STR = new ListValType(new String[] { Flags.JSON_STR, "json", "j" }, STR);
	public static final ListValType JSONARRAY_STR=new ListValType(new String[] { Flags.JSONARRAY_STR, "json", "j" }, STR);
	public static final ListValType GEO_PG_GEOMETRY =new ListValType(new String[] { Flags.GEO_PG_GEOMETRY, "geo_pg_geometry", "g" }, STR);
	public static final ListValType GEO_SHAPE=new ListValType(new String[] { Flags.GEO_SHAPE, "geo_shape", "g" }, STR);
	
	public final Class<?> rawClass;
	public final Class<?> boxedClass;
	public final int jdbcType;
	public final String flag;

	protected ValType(Class<?> rawClass, Class<?> boxedClass, int jdbcType, String... flags) {
		this.rawClass = rawClass;
		this.boxedClass = boxedClass;
		this.jdbcType = jdbcType;
		String f = null;
		if (null != flags && flags.length > 0) {
			f = flags[0];
			for (int i = 0; i < flags.length; i++)
				if (null != flags[i]) MAPPING.putIfAbsent(flags[i], this);
		}
		flag = f;
	}

	@Override
	public String toString() {
		return flag;
	}

	public ValType len(long len) {
		return this;
	}

	public static ValType of(String flag) {
		return null == flag ? UNKNOWN : MAPPING.getOrDefault(flag, UNKNOWN);
	}

	public static ValType obj(Object obj) {
		if (null == obj) return VOID;
		Class<?> cls = obj.getClass();
		return cls(cls);
	}

	public static ValType cls(Class<?> cls) {
		if (Boolean.class.isAssignableFrom(cls) || boolean.class.isAssignableFrom(cls)) return BOOL;
		if (Character.class.isAssignableFrom(cls) || char.class.isAssignableFrom(cls)) return CHAR;
		if (Byte.class.isAssignableFrom(cls) || byte.class.isAssignableFrom(cls)) return BYTE;
		if (Short.class.isAssignableFrom(cls) || short.class.isAssignableFrom(cls)) return SHORT;
		if (Integer.class.isAssignableFrom(cls) || int.class.isAssignableFrom(cls)) return INT;
		if (Long.class.isAssignableFrom(cls) || long.class.isAssignableFrom(cls)) return LONG;
		if (Float.class.isAssignableFrom(cls) || float.class.isAssignableFrom(cls)) return FLOAT;
		if (Double.class.isAssignableFrom(cls) || double.class.isAssignableFrom(cls)) return DOUBLE;
		if (CharSequence.class.isAssignableFrom(cls)) return STR;
		if (byte[].class.isAssignableFrom(cls)) return BIN;
		if (Date.class.isAssignableFrom(cls)) return DATE;
		if (Timestamp.class.isAssignableFrom(cls)) return TIMESTAMP;
		return UNKNOWN;
	}
}
