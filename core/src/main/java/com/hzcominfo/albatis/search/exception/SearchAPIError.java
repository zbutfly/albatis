package com.hzcominfo.albatis.search.exception;

/**
 * Created by ljx on 2016/11/23.
 */
public class SearchAPIError extends Error {
	private static final long serialVersionUID = 7986280933676921893L;

	public SearchAPIError() {}

	public SearchAPIError(String message) {
		super(message);
	}

	public SearchAPIError(Throwable cause) {
		super(cause);
	}

	public SearchAPIError(String message, Throwable cause) {
		super(message, cause);
	}

}
