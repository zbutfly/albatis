package com.hzcominfo.albatis.nosql;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

@Deprecated
interface Connect {
	static final Logger logger = Logger.getLogger(Connect.class);

	static <T extends Connection> T connect(URISpec uri, Class<T> clazz) throws IOException {
		return connect(uri, clazz, new HashMap<>());
	}

	static <T extends Connection> T connect(URISpec uri, Class<T> clazz, String... params) throws IOException {
		Pair<String, Object[]> p = Maps.parseFirstKey((Object[]) params);
		return connect(uri, clazz, Maps.of(p.v1(), p.v2()));
	}

	static <T extends Connection> T connect(URISpec uriSpec, Class<T> clazz, Map<String, String> props) throws IOException {
		try {
			return clazz.getConstructor(new Class[] { URISpec.class }).newInstance(uriSpec);
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			Throwable ee = e.getTargetException();
			if (e instanceof InvocationTargetException && e.getTargetException() instanceof IOException) throw (IOException) ee;
			throw new RuntimeException(e.getTargetException());
		}
	}

	static boolean sparking() {
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
