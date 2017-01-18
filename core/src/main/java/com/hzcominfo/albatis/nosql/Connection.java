package com.hzcominfo.albatis.nosql;

import net.butfly.albacore.io.URISpec;

public interface Connection extends AutoCloseable {
	String defaultSchema();

	URISpec getURI();
}
