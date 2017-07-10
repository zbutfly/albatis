package com.hzcominfo.albatis.nosql;

import java.util.concurrent.ConcurrentSkipListMap;

import net.butfly.albacore.io.utils.URISpec;

import net.butfly.albacore.io.URISpec;

public interface Connection extends AutoCloseable {
	public static final String PARAM_KEY_BATCH = "batch";
	public static final int DEFAULT_BATCH_SIZE = 500;

	class _Wrap {
		private static final ConcurrentMap<String, ConcurrentSkipListSet<Class<? extends Connection>>> drivers = new ConcurrentHashMap<>();
		private static final ConcurrentMap<Class<? extends Connection>, ConcurrentSkipListSet<String>> schemas = new ConcurrentHashMap<>();
	}

	String defaultSchema();

	URISpec getURI();

	public Connection connection(String url) throws Exception;

	public Connection connection(URISpec uriSpec) throws Exception;

	class _Private {
		private static final ConcurrentSkipListMap<String, Class<?>> registerMap = new ConcurrentSkipListMap<>();
	}

	public static void register(String schema, Class<?> clazzName) {
		_Private.registerMap.put(schema, clazzName);
	}

	public static Class<?> getRegisterInfo(String schema) {
		return _Private.registerMap.get(schema);
	}
}
