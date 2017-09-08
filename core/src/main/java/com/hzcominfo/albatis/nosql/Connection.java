package com.hzcominfo.albatis.nosql;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

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

	static Connection connect(URISpec spec) {
		return null;
	}

	static void register(Class<? extends Connection> c, String... schemas) {
		for (String s : schemas)
			_Wrap.drivers.compute(s, (schema, classes) -> {
				if (null == classes) classes = new ConcurrentSkipListSet<>();
				classes.add(c);
				return classes;
			});
		_Wrap.schemas.compute(c, (cc, ss) -> {
			if (null == ss) ss = new ConcurrentSkipListSet<>();
			ss.addAll(Arrays.asList(schemas));
			return ss;
		});
	}
}
