package com.hzcominfo.dataggr.uniquery.field_mask;

import java.lang.reflect.Proxy;
import java.sql.SQLException;

import com.hzcominfo.albatis.search.auth.AuthHandler;
import com.hzcominfo.albatis.search.result.ResultSetBase;

public class FieldHander implements AuthHandler{
	private final static Class<?>[] intfs = new Class[] { ResultSetBase.class };

	@Override
	public ResultSetBase auth(ResultSetBase rs, String uri, String[] fArray, String authKey)
			throws IllegalArgumentException, SQLException {
		return (ResultSetBase) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), intfs,
				new ResultSetHandler(rs, uri, fArray, authKey));
		
	}
	
}
