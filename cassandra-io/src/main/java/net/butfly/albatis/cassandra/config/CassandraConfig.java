package net.butfly.albatis.cassandra.config;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;

import net.butfly.albacore.io.URISpec;

public class CassandraConfig {
	private static long connectionMaxRequests;
	private static long maxConcurrentRequests;
//	private boolean throttler = false;

	public static DriverConfigLoader buildConfig(URISpec uri) {
		ProgrammaticDriverConfigLoaderBuilder builder = DriverConfigLoader.programmaticBuilder();
		
		if (null != uri.getParameter("connectionMaxRequests")) {
			connectionMaxRequests = Long.parseLong(uri.fetchParameter("connectionMaxRequests"));
//			throttler = true;
			builder.withLong(DefaultDriverOption.CONNECTION_MAX_REQUESTS, connectionMaxRequests);
		}
		if (null != uri.getParameter("maxConcurrentRequests")) {
			maxConcurrentRequests = Long.parseLong(uri.fetchParameter("maxConcurrentRequests"));
			
			builder.withLong(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS, maxConcurrentRequests);
		}
		return builder.build();
	}
}
