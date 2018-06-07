package com.hzcominfo.dataggr.spark.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.io.URISpec;

public class FuncUtil implements Serializable {
	private static final long serialVersionUID = -8305619702897096234L;

	public static Consumer<Row> func = row -> {
		StructType schema = row.schema();
		String[] fieldNames = schema.fieldNames();
		Map<String, Object> map = new HashMap<>();
		for (String fn : fieldNames) {
			map.put(fn, row.getAs(fn));
		}
		System.out.println(map);
	};

	public static Function<URISpec, String> defaultcoll = u -> {
		String file = u.getFile();
		String[] path = u.getPaths();
		String tbl = null;
		if (path.length > 0)
			tbl = file;
		return tbl;
	};
}
