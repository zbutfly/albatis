package com.hzcominfo.dataggr.spark.util;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class FuncUtil implements Serializable {
	private static final long serialVersionUID = -8305619702897096234L;

	//
	// public static final Encoder<Map> MAP_ENC = Encoders.bean(Map.class);

	public static Function<URISpec, String> defaultcoll = u -> {
		String file = u.getFile();
		String[] path = u.getPaths();
		String tbl = null;
		if (path.length > 0) tbl = file;
		return tbl;
	};

	public static Map<String, Object> rowMap(Row row) {
		return JavaConversions.mapAsJavaMap(//
				row.getValuesMap(//
						JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala().toSeq()));
	}

	private static final DataType classType(Object v) {
		return classType(null == v ? Void.class : v.getClass());
	}

	private static final DataType classType(Class<?> c) {
		if (CharSequence.class.isAssignableFrom(c)) return DataTypes.StringType;
		if (int.class.isAssignableFrom(c) && Integer.class.isAssignableFrom(c)) return DataTypes.IntegerType;
		if (long.class.isAssignableFrom(c) && Long.class.isAssignableFrom(c)) return DataTypes.LongType;
		if (boolean.class.isAssignableFrom(c) && Boolean.class.isAssignableFrom(c)) return DataTypes.BooleanType;
		if (double.class.isAssignableFrom(c) && Double.class.isAssignableFrom(c)) return DataTypes.DoubleType;
		if (float.class.isAssignableFrom(c) && Float.class.isAssignableFrom(c)) return DataTypes.FloatType;
		if (byte.class.isAssignableFrom(c) && Byte.class.isAssignableFrom(c)) return DataTypes.ByteType;
		if (short.class.isAssignableFrom(c) && Short.class.isAssignableFrom(c)) return DataTypes.ShortType;
		if (byte[].class.isAssignableFrom(c)) return DataTypes.BinaryType;
		if (Date.class.isAssignableFrom(c)) return DataTypes.DateType;
		if (Timestamp.class.isAssignableFrom(c)) return DataTypes.TimestampType;
		if (Void.class.isAssignableFrom(c)) return DataTypes.NullType;
		// if (CharSequence.class.isAssignableFrom(c)) return DataTypes.CalendarIntervalType;
		if (c.isArray()) return DataTypes.createArrayType(classType(c.getComponentType()));
		// if (Iterable.class.isAssignableFrom(c)) return DataTypes.createArrayType(elementType);
		// if (Map.class.isAssignableFrom(c)) return DataTypes.createMapType(keyType, valueType);
		throw new UnsupportedOperationException(c.getName() + " not support for spark sql data type");
	}

	public static Row mapRow(Map<String, Object> map) {
		List<StructField> fields = Colls.list();
		map.forEach((k, v) -> fields.add(DataTypes.createStructField(k, classType(v), null == v)));
		return new GenericRowWithSchema(map.values().toArray(), DataTypes.createStructType(fields));
	}

	public static <T> Seq<T> dataset(Sdream<T> rows) {
		return JavaConverters.asScalaIteratorConverter(rows.list().iterator()).asScala().toSeq();
	}

	// public Dataset<Map> mapize(Dataset<Row> ds) {
	// TypeTag<Map> tag = null;
	// Encoder<Map<?, ?>> enc = spark.implicits().newMapEncoder(tag);
	// Dataset<scala.collection.Map<?, ?>> dss = ds.map(r -> null, enc);
	// }
}
