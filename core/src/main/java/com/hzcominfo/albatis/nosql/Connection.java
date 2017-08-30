package com.hzcominfo.albatis.nosql;

import net.butfly.albacore.io.URISpec;

public interface Connection extends AutoCloseable {
	public static final String PARAM_KEY_BATCH = "batch";

	String defaultSchema();

	URISpec getURI();
}
