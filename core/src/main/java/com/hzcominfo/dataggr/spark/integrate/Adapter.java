package com.hzcominfo.dataggr.spark.integrate;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.integrate.util.ExceptionUtil;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;

public abstract class Adapter implements Serializable {
	private static final long serialVersionUID = -7449826645946027806L;

	public Adapter() {}

	static final Map<String, Class<? extends Adapter>> ADAPTER_MAP = loadAdapters();

	public abstract Dataset<Row> read(URISpec uriSpec, SparkSession spark);
	
	// 加载子类
	static Map<String, Class<? extends Adapter>> loadAdapters() {
		Map<String, Class<? extends Adapter>> map = new HashMap<>();
		Set<Class<? extends Adapter>> set = Reflections.getSubClasses(Adapter.class);
		for (Class<? extends Adapter> c : set) {
			try {
				Field[] fields = c.getDeclaredFields();
				if (fields != null && fields.length > 0) {
					for (Field f : fields) {
						if ("schema".equals(f.getName())) {
							f.setAccessible(true);
							map.put((String) f.get(c), c);
							break;
						}
					}
				}
			} catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException("get field error", e);
			}
		}
		return map;
	};

	// 根据uriSpec获取对应的适配器实现
	@SuppressWarnings("unchecked")
	public static <T extends Adapter> T adapt(URISpec uriSpec) {
		for (String schema : ADAPTER_MAP.keySet()) {
			if (schema.contains(uriSpec.getScheme())) {
				try {
					return (T) adapt(ADAPTER_MAP.get(schema));
				} catch (Exception e) {
					ExceptionUtil.runtime("Constructor error: ", e);
				}
			}
		}
		throw new RuntimeException("No matched adapter");
	}

	static <T extends Adapter> T adapt(Class<T> clazz) throws Exception {
		Constructor<T> con = clazz.getConstructor();
		return con.newInstance();
	}
}
