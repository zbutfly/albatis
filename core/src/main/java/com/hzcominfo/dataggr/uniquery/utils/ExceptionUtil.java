package com.hzcominfo.dataggr.uniquery.utils;

public class ExceptionUtil {

	public static void runtime(String msg, Exception e) {
		throw new RuntimeException(msg + ": " + e);
	}
}
