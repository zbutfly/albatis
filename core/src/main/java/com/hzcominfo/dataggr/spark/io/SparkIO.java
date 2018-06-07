package com.hzcominfo.dataggr.spark.io;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;

public abstract class SparkIO {
	private static final Map<String, Class<? extends SparkIO>> ADAPTER_MAP = loadAdapters();

	protected final SparkSession spark;
	protected final URISpec targetUri;

	protected SparkIO(SparkSession spark, URISpec targetUri) {
		super();
		this.spark = spark;
		this.targetUri = targetUri;
	}

	private static Map<String, Class<? extends SparkIO>> loadAdapters() {
		Map<String, Class<? extends SparkIO>> map = new HashMap<>();
		for (Class<? extends SparkIO> c : Reflections.getSubClasses(SparkIO.class))
			try {
				Field[] fields = c.getDeclaredFields();
				if (fields != null && fields.length > 0) for (Field f : fields)
					if ("schema".equals(f.getName())) {
						f.setAccessible(true);
						map.put((String) f.get(c), c);
						break;
					}
			} catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException("get field error", e);
			}
		return map;
	}

	public static <O extends SparkOutput> O output(SparkSession spark, URISpec uri) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> c = (Class<O>) ADAPTER_MAP.get(s);
			if (null == c) s = s.substring(0, s.lastIndexOf(':'));
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class).newInstance(spark, uri);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		throw new RuntimeException("No matched adapter");
	}

	public static <I extends SparkInput> I input(SparkSession spark, URISpec uri) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> c = (Class<I>) ADAPTER_MAP.get(s);
			if (null == c) s = s.substring(0, s.lastIndexOf(':'));
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class).newInstance(spark, uri);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		throw new RuntimeException("No matched adapter");
	}

	protected abstract Map<String, String> options(URISpec uriSpec);
}
