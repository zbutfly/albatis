package com.hzcominfo.albatis.nosql;

import java.lang.reflect.Constructor;
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

	public static Connection connect(String url, Map<String, String> props) throws Exception {
		URISpec uriSpec = new URISpec(url);
		return connect(uriSpec, props);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Connection connect(URISpec uriSpec, Map<String, String> props) throws Exception {
		Class clazz;
		Constructor con = null;
		Connection connection = null;
		switch (uriSpec.getScheme()) {
			case "mongodb":
				clazz = Connection.getRegisterInfo("mongodb");
				con = clazz.getConstructor(new Class[] { URISpec.class });
				connection = (Connection) con.newInstance(uriSpec);
				break;
			case "kudu":
				clazz = Connection.getRegisterInfo("kudu");
				con = clazz.getConstructor(new Class[] { URISpec.class, Map.class });
				connection = (Connection) con.newInstance(uriSpec, props);
				break;
			case "es":
				clazz = Connection.getRegisterInfo("es");
				con = clazz.getConstructor(new Class[] { URISpec.class, Map.class });
				connection = (Connection) con.newInstance(uriSpec, props);
				break;
			case "hbase":
				clazz = Connection.getRegisterInfo("hbase");
				con = clazz.getConstructor(new Class[] { URISpec.class });
				connection = (Connection) con.newInstance(uriSpec);
				break;
			case "zk:kafka":
				
				break;
			case "zk:solr":
				clazz = Connection.getRegisterInfo("zk:solr");
				con = clazz.getConstructor(new Class[] { URISpec.class });
				connection = (Connection) con.newInstance(uriSpec);
				break;
			default:
				break;
		}
		return connection;
	}
}
