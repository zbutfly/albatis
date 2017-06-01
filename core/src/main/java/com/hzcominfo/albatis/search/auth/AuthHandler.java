package com.hzcominfo.albatis.search.auth;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.hzcominfo.albatis.search.result.ResultSetBase;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;

public interface AuthHandler {
	static final Logger logger = Logger.getLogger(AuthHandler.class);

	ResultSetBase auth(ResultSetBase rs, String uri, String[] fList, String authKey) throws IllegalArgumentException, SQLException;

	static AuthHandler scan() {
		List<Class<? extends AuthHandler>> cs = new ArrayList<>(Reflections.getSubClasses(AuthHandler.class));
		if (cs.size() == 0)
			return null;
		Class<? extends AuthHandler> c = cs.get(0);
		if (cs.size() > 1)
			logger.warn("Multiple binding of auth handler found, use first: " + c.toString());
		return Reflections.construct(c);
	}
}
