package com.hzcominfo.dataggr.spark.io;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;

public abstract class SparkIO {
	private static final Map<String, Class<? extends SparkInput>> ADAPTER_INPUT = loadInputAdapters();
	private static final Map<String, Class<? extends SparkOutput>> ADAPTER_OUTPUT = loadOutputAdapters();

	protected SparkSession spark;
	protected URISpec targetUri;

	public SparkIO() {}

	protected SparkIO(SparkSession spark, URISpec targetUri) {
		super();
		this.spark = spark;
		this.targetUri = targetUri;
	}

	private static Map<String, Class<? extends SparkInput>> loadInputAdapters() {
		Map<String, Class<? extends SparkInput>> map = new HashMap<>();
		for (Class<? extends SparkInput> c : Reflections.getSubClasses(SparkInput.class))
			loadAdapters(map, c);
		return map;
	}

	private static Map<String, Class<? extends SparkOutput>> loadOutputAdapters() {
		Map<String, Class<? extends SparkOutput>> map = new HashMap<>();
		for (Class<? extends SparkOutput> c : Reflections.getSubClasses(SparkOutput.class))
			loadAdapters(map, c);
		return map;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void loadAdapters(Map map, Class<?> c) {
		try {
			Field[] fields = c.getDeclaredFields();
			if (fields != null && fields.length > 0) for (Field f : fields)
				if ("schema".equals(f.getName())) {
					f.setAccessible(true);
					String schema = (String) f.get(c);
					if (schema.contains(",")) {
						String[] split = schema.split(",");
						for (String s : split)
							map.put(s, c);
					} else map.put(schema, c);
					break;
				}
		} catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException("get field error", e);
		}
	}

	public static <O extends SparkOutput> O output(SparkSession spark, URISpec uri) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> c = (Class<O>) ADAPTER_OUTPUT.get(s);
			if (null == c) break;
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class).newInstance(spark, uri);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		throw new RuntimeException("No matched adapter with scheme: " + s);
	}

	public static <I extends SparkInput> I input(SparkSession spark, URISpec uri, String... fields) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> c = (Class<I>) ADAPTER_INPUT.get(s);
			if (null == c) s = s.substring(0, s.lastIndexOf(':'));
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, fields);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		throw new RuntimeException("No matched adapter with scheme: " + s);
	}

	protected abstract Map<String, String> options(URISpec uriSpec);
}
