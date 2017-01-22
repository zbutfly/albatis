package com.hzcominfo.albatis.search.exception;

import java.sql.SQLException;

/**
 * API系统中处理的最基本Exception，建议本包其他的Exception自此继承而来
 */
public class SearchAPIException extends SQLException {
	private static final long serialVersionUID = -2892720118467579984L;

	public SearchAPIException() {
		super();
	}

	public SearchAPIException(String message) {
		super(message);
	}

	public SearchAPIException(String message, Throwable cause) {
		super(message, cause);
	}

	public SearchAPIException(Throwable cause) {
		super(cause);
	}
}
