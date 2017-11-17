package com.hzcominfo.albatis.nosql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Maps;

public interface Connection extends AutoCloseable {
	public static final String PARAM_KEY_BATCH = "batch";
	public static final int DEFAULT_BATCH_SIZE = 500;
	static final Map<String, Class<? extends Connection>> CONNECTION_MAP = loadConnections();

	String defaultSchema();

	URISpec getURI();

	@SuppressWarnings("unchecked")
	public static <T extends Connection> T connect(URISpec uriSpec) throws Exception {
		for (String schema : CONNECTION_MAP.keySet()) {
			if (schema.contains(uriSpec.getScheme())) {
				return (T) connect(uriSpec, CONNECTION_MAP.get(schema));
			}
		}
		throw new RuntimeException("No matched connection");
	}

	public static <T extends Connection> T connect(String url, Class<T> clazz) throws Exception {
		return connect(new URISpec(url), clazz);
	}

	public static <T extends Connection> T connect(String url, Class<T> clazz, String... params) throws Exception {
		Pair<String, Object[]> p = Maps.parseFirstKey((Object[]) params);
		return connect(new URISpec(url), clazz, Maps.of(p.v1(), p.v2()));
	}

	public static <T extends Connection> T connect(String url, Class<T> clazz, Map<String, String> props) throws Exception {
		return connect(new URISpec(url), clazz, props);
	}

	public static <T extends Connection> T connect(URISpec uri, Class<T> clazz) throws Exception {
		return connect(uri, clazz, new HashMap<>());
	}

	public static <T extends Connection> T connect(URISpec uri, Class<T> clazz, String... params) throws Exception {
		Pair<String, Object[]> p = Maps.parseFirstKey((Object[]) params);
		return connect(uri, clazz, Maps.of(p.v1(), p.v2()));
	}

	public static <T extends Connection> T connect(URISpec uriSpec, Class<T> clazz, Map<String, String> props) throws Exception {
		Constructor<T> con = clazz.getConstructor(new Class[] { URISpec.class });
		return con.newInstance(uriSpec);
	}

	public static Map<String, Class<? extends Connection>> loadConnections() {
		Map<String, Class<? extends Connection>> map = new HashMap<>();
		Set<Class<? extends Connection>> set = Reflections.getSubClasses(Connection.class);
		for (Class<? extends Connection> c : set) {
			try {
				if (c.getSuperclass().isAssignableFrom(NoSqlConnection.class)) {
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
				}
			} catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException("get field error", e);
			}
		}
		return map;
	}
}
