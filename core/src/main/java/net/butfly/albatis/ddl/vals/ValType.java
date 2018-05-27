package net.butfly.albatis.ddl.vals;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Date;
import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;

public abstract class ValType {
	protected final static Map<String, ValType> MAPPING = Maps.of();

	public static interface Flags {
		@Deprecated
		public static final String VOID = "v";
		@Deprecated
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
		// assembly type
		public static final String GEO = "g";
	}

	@Deprecated
	public static final BasicValType VOID = new BasicValType(void.class, Void.class, Types.OTHER, Flags.VOID, "void");
	@Deprecated
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
			"datetime", "timestamp");
	// assembly type
	public static final ListValType GEO = new ListValType(new String[] { Flags.GEO, "geo" }, DOUBLE, DOUBLE);

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

	public static ValType valueOf(String flag) {
		return null == flag ? UNKNOWN : MAPPING.getOrDefault(flag, UNKNOWN);
	}

}
