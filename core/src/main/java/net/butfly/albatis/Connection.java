package net.butfly.albatis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IOFactory;

public interface Connection extends AutoCloseable, IOFactory {
	static final Logger logger = Logger.getLogger(Connection.class);

	static final String PARAM_KEY_BATCH = "batch";
	static final int DEFAULT_BATCH_SIZE = 500;
	static final Connection DUMMY = new Connection() {
		@Override
		public void close() throws IOException {}

		@Override
		public String defaultSchema() {
			return null;
		}

		@Override
		public URISpec uri() {
			return null;
		}
	};

	String defaultSchema();

	@Override
	URISpec uri();

	@Override
	void close() throws IOException;

	@SuppressWarnings("unchecked")
	static <T extends Connection> T connect(URISpec uri) throws IOException {
		return (T) Environment.connect(uri);
	}

	interface Driver<C extends Connection> {
		List<String> schemas();

		C connect(URISpec uriSpec) throws IOException;
	}

	@SuppressWarnings("rawtypes")
	class DriverManager {
		private final static Map<String, Driver> DRIVERS = Maps.of();

		static {
			for (Driver driver : ServiceLoader.load(Driver.class)) {
				// XXX why can't I do register here?
				// for (String schema : (List<String>) driver.schemas())
				// DRIVERS.put(schema, driver);
				Class<?> c;
				try {
					c = driver.getClass().getEnclosingClass();
					_Logger.logger.debug("Connection driver [" + c.toString() + "] loaded as schema " + driver.schemas());
				} catch (NoClassDefFoundError e) {
					_Logger.logger.warn("Connection driver [" + driver.getClass().toString() + "] found but driver invalid, schema "
							+ driver.schemas() + " should not be used, cause: " + e.getMessage());
				}
			}
		}

		@SuppressWarnings("unchecked")
		public static void register(Driver driver) {
			for (String schema : (List<String>) driver.schemas())
				DRIVERS.put(schema, driver);
		}

		@SuppressWarnings("unchecked")
		public static <T extends Connection> T connect(URISpec uriSpec) throws IOException {
			String s = uriSpec.getSchema();
			Driver d;
			while (!s.isEmpty()) {
				if (null != (d = DRIVERS.get(s))) return (T) d.connect(uriSpec);
				else {
					int c = s.lastIndexOf(":");
					if (c >= 0) s = s.substring(0, c);
					else break;
				}
			}
			throw new RuntimeException("No matched connection for schema [" + uriSpec.getSchema() + "]");
		}
	}

	// ddl
	default void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		logger().warn("Constructing invoked but not implemented, ignore.");
	}

	default void construct(String table, String aliasName, TableDesc tableDesc, List<FieldDesc> fields) {
		logger().warn("Constructing invoked but not implemented, ignore.");
	}

	default void construct(String table, FieldDesc... fields) {
		logger().warn("Constructing invoked but not implemented, ignore.");
	}

	default void construct(Qualifier qualifier, FieldDesc... fields) {
		logger().warn("Constructing invoked but not implemented, ignore.");
	}

	default void construct(Map<String, Object> tableConfig, FieldDesc... fields) {
		logger().warn("Constructing invoked but not implemented, ignore.");
	}

	default void construct(Map<String, Object> mapping, String table) {
		logger().warn("Constructing invoked but not implemented, ignore.");
	}

	default void alterFields(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		logger().warn("Altering fields invoked but not implemented, ignore.");
	}

	default List<Map<String, Object>> getResultListByCondition(String table, Map<String, Object> condition) {
		throw new UnsupportedOperationException("Getting results by condition invoked but not implemented, ignore.");
	}

	default void deleteByCondition(String table, Map<String, Object> condition) {
		logger().warn("Deleting by condition invoked but not implemented, ignore.");
	}

	/**
	 * @param table
	 * @return whether the table existed or not
	 */
	default boolean judge(String table) {
		throw new NotImplementedException();
	}

	default void destruct(String table) {
		throw new UnsupportedOperationException();
	}

	default void truncate(String table) {
		throw new UnsupportedOperationException();
	}

	// utils
	@Deprecated
	static Function<Map<String, Object>, byte[]> uriser(URISpec uri) {
		String sd = null == uri ? "bson" : uri.getParameter("serder", "bson").toLowerCase();
		switch (sd) {
		case "bson":
			return BsonSerder::map;
		case "json":
			return m -> JsonSerder.JSON_MAPPER.ser(m).getBytes(StandardCharsets.UTF_8);
		default:
			_Logger.logger.warn("Current only support \"serder=bson|json\", \"" + sd + "\" is not supported, use bson by failback.");
			return BsonSerder::map;
		}
	}

	@Deprecated
	static Function<byte[], Map<String, Object>> urider(URISpec uri) {
		String sd = null == uri ? "bson" : uri.getParameter("serder", "bson").toLowerCase();
		switch (sd) {
		case "bson":
			return BsonSerder::map;
		case "json":
			return m -> JsonSerder.JSON_MAPPER.der(new String(m, StandardCharsets.UTF_8));
		default:
			_Logger.logger.warn("Current only support \"serder=bson|json\", \"" + sd + "\" is not supported, use bson by failback.");
			return BsonSerder::map;
		}
	}

	@Deprecated
	static Function<byte[], List<Map<String, Object>>> uriders(URISpec uri) {
		String sd = null == uri ? "bson" : uri.getParameter("serder", "bson").toLowerCase();
		switch (sd) {
		case "bson":
			return BsonSerder::maps;
		case "json":
			return m -> JsonSerder.JSON_MAPPER.ders(new String(m, StandardCharsets.UTF_8));
		default:
			_Logger.logger.warn("Current only support \"serder=bson|json\", \"" + sd + "\" is not supported, use bson by failback.");
			return BsonSerder::maps;
		}
	}

	@Deprecated
	static Function<String, List<Map<String, Object>>> strUriders(URISpec uri) {
		String sd = null == uri ? "bson" : uri.getParameter("serder", "bson").toLowerCase();
		switch (sd) {
		case "json":
			return m -> JsonSerder.JSON_MAPPER.ders(m);
		default:
			_Logger.logger.warn("Current only support \"serder=bson|json\", \"" + sd + "\" is not supported, use bson by failback.");
			return m -> JsonSerder.JSON_MAPPER.ders(m);
		}
	}

	class _Logger {
		private static final Logger logger = Logger.getLogger(Connection.class);
	}

	static final boolean SPARING = _Priv.sparking();

	class _Priv {
		private static boolean sparking() {
			try {
				Class.forName("net.butfly.albatis.spark.impl.SparkConnection");
				String suri = System.getProperty("albatis.io.env.uri", "spark:///");
				logger.info("Spark IO classloader found, init the spark env: " + suri);
				System.setProperty("albatis.io.env.uri", suri);
				return true;
			} catch (ClassNotFoundException e) {
				return false;
			}
		}
	}
}
