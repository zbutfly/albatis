package com.hzcominfo.dataggr.spark.integrate.util;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class FuncUtil {

	public static Consumer<Row> func = row -> {
		StructType schema = row.schema();
		String[] fieldNames = schema.fieldNames();
		Map<String, Object> map = new HashMap<>();
		for (String fn : fieldNames) {
			map.put(fn, row.getAs(fn));
		}
		System.out.println(map);
	};
}
