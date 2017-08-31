package com.hzcominfo.albatis.nosql;

import net.butfly.albacore.io.URISpec;

public interface Connection extends AutoCloseable {
	public static final String PARAM_KEY_BATCH = "batch";
	public static final int DEFAULT_BATCH_SIZE = 500;

	String defaultSchema();

	URISpec getURI();
}
