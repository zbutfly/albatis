package com.hzcominfo.albatis.nosql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import net.butfly.albacore.io.utils.URISpec;

public interface Connection extends AutoCloseable {
	public static final String PARAM_KEY_BATCH = "batch";
	public static final int DEFAULT_BATCH_SIZE = 500;

	String defaultSchema();

	URISpec getURI();

	class _Private {
		private static final ConcurrentSkipListMap<String, Class<?>> registerMap = new ConcurrentSkipListMap<>();
	}

	public static void register(String schema, Class<?> clazzName) {
		_Private.registerMap.put(schema, clazzName);
	}

	public static Class<?> getRegisterInfo(String schema) {
		return _Private.registerMap.get(schema);
	}

	public static Connection connection(String url, Map<String, String> props) throws Exception {
		URISpec uriSpec = new URISpec(url);
		return connection(uriSpec, props);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Connection connection(URISpec uriSpec, Map<String, String> props) throws Exception {
		Class clazz;
		Connection connection = null;
		if (uriSpec.getScheme().equalsIgnoreCase("mongodb")) {
			clazz = Connection.getRegisterInfo("mongodb");
			Constructor con = clazz.getConstructor(new Class[] { URISpec.class });
			connection = (Connection) con.newInstance(uriSpec);
		} else if (uriSpec.getScheme().equalsIgnoreCase("kudu")) {
			clazz = Connection.getRegisterInfo("kudu");
			Constructor con = clazz.getConstructor(new Class[] { URISpec.class, Map.class });
			connection = (Connection) con.newInstance(uriSpec, props);
		} else if (uriSpec.getScheme().equalsIgnoreCase("es")) {
			clazz = Connection.getRegisterInfo("es");
			Constructor con = clazz.getConstructor(new Class[] { URISpec.class, Map.class });
			connection = (Connection) con.newInstance(uriSpec, props);
		} else if (uriSpec.getScheme().equalsIgnoreCase("hbase")) {
			clazz = Connection.getRegisterInfo("hbase");
			Constructor con = clazz.getConstructor(new Class[] { URISpec.class });
			connection = (Connection) con.newInstance(uriSpec);
		}
		return connection;
	}
}
