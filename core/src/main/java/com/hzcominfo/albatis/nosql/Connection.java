package com.hzcominfo.albatis.nosql;

public interface Connection extends AutoCloseable {
	String defaultSchema();
}
