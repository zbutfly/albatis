package com.hzcominfo.albatis.nosql;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.R;
import net.butfly.albatis.io.Output;

public interface Connection extends AutoCloseable {
	public static final String PARAM_KEY_BATCH = "batch";
	public static final int DEFAULT_BATCH_SIZE = 500;
	public static final Logger logger = Logger.getLogger(Connection.class);

	String defaultSchema();

	URISpec uri();

	static <T extends Connection> T connect(URISpec uriSpec) throws IOException {
		return DriverManager.connect(uriSpec);
	}

	<M extends R> Input<M> input(String... table) throws IOException;

	<M extends R> Output<M> output() throws IOException;

	interface Driver<C extends Connection> {
		List<String> schemas();

		C connect(URISpec uriSpec) throws IOException;

		// Class<C> connectClass();
	}

	@SuppressWarnings("rawtypes")
	class DriverManager {
		private final static Map<String, Driver> DRIVERS = Maps.of();
		static {
			for (Driver driver : ServiceLoader.load(Driver.class)) {
				// XXX why can't I do register here?
				// for (String schema : (List<String>) driver.schemas())
				// DRIVERS.put(schema, driver);
				logger.debug("Connection driver loaded: " + driver.getClass().toString());
			}
		}

		@SuppressWarnings("unchecked")
		public static void register(Driver driver) {
			for (String schema : (List<String>) driver.schemas())
				DRIVERS.put(schema, driver);
		}

		@SuppressWarnings("unchecked")
		static <T extends Connection> T connect(URISpec uriSpec) throws IOException {
			String s = uriSpec.getScheme();
			Driver d;
			while (!s.isEmpty()) {
				if (null != (d = DRIVERS.get(s))) return (T) d.connect(uriSpec);
				else {
					int c = s.lastIndexOf(":");
					if (c >= 0) s = s.substring(0, c);
					else break;
				}
			}
			// return Connect.connect(uriSpec, d.connectClass());
			throw new RuntimeException("No matched connection for schema [" + uriSpec.getScheme() + "]");
		}
	}

	@Deprecated
	class Connect {
		public static <T extends Connection> T connect(URISpec uri, Class<T> clazz) throws IOException {
			return connect(uri, clazz, new HashMap<>());
		}

		public static <T extends Connection> T connect(URISpec uri, Class<T> clazz, String... params) throws IOException {
			Pair<String, Object[]> p = Maps.parseFirstKey((Object[]) params);
			return connect(uri, clazz, Maps.of(p.v1(), p.v2()));
		}

		public static <T extends Connection> T connect(URISpec uriSpec, Class<T> clazz, Map<String, String> props) throws IOException {
			try {
				return clazz.getConstructor(new Class[] { URISpec.class }).newInstance(uriSpec);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
					| IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				Throwable ee = e.getTargetException();
				if (e instanceof InvocationTargetException && e.getTargetException() instanceof IOException) throw (IOException) ee;
				throw new RuntimeException(e.getTargetException());
			}
		}
	}

	// DDL
	default void construct(String table, FieldDesc... fields) {
		throw new UnsupportedOperationException();
	}

	default void destruct(String table) {
		throw new UnsupportedOperationException();
	}

	default void truncate(String table) {
		throw new UnsupportedOperationException();
	}
}
