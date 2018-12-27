package com.hzcominfo.albatis.nosql;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;

public interface Connection extends AutoCloseable {
	public static final String PARAM_KEY_BATCH = "batch";
	public static final int DEFAULT_BATCH_SIZE = 500;

	String defaultSchema();

	URISpec getURI();

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
}
