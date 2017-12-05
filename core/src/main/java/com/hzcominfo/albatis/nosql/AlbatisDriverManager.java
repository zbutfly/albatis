package com.hzcominfo.albatis.nosql;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.io.URISpec;

public class AlbatisDriverManager {

	private final static Map<String, AlbatisDriver> registeredDrivers = new ConcurrentHashMap<>();
	static {
		loadConnections();
        System.out.println("ConnectionManager initialized");
	}

	private static void loadConnections() {
		ServiceLoader<AlbatisDriver> loadedConnections = ServiceLoader.load(AlbatisDriver.class);
		Iterator<AlbatisDriver> driversIterator = loadedConnections.iterator();
		while (driversIterator.hasNext()) {
			driversIterator.next();
		}
	}

	public synchronized static void registerDriver(String key, AlbatisDriver driver) {
		registeredDrivers.put(key, driver);
	}

	@SuppressWarnings("unchecked")
	public static <T extends Connection> T connect(URISpec uriSpec) throws Exception {
		for (String schema : registeredDrivers.keySet()) {
			if (schema.contains(uriSpec.getScheme())) {
				return (T) registeredDrivers.get(schema).connect(uriSpec);
			}
		}
		throw new RuntimeException("No matched connection");
	}
}
