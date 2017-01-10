package com.hzcominfo.albatis.nosql;

import java.io.IOException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

public abstract class NoSqlConnection<C> implements Connection {
	protected final String[] supportedSchemas;
	private final C client;
	protected final URI uri;
	protected final Map<String, String> parameters;

	protected NoSqlConnection(String connection, String... supportedSchema) throws IOException {
		super();
		supportedSchemas = null != supportedSchema ? supportedSchema : new String[0];
		try {
			uri = new URI(connection);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		String schema = supportedSchema(uri.getScheme());
		if (null == schema) throw new ProtocolException(uri.getScheme() + " is not supported, "//
				+ "supported list: [" + Joiner.on(',').join(supportedSchemas) + "]");
		client = createClient(uri);
		parameters = new HashMap<>();
		for (String param : uri.getQuery().split("&")) {
			String[] p = param.split("=", 2);
			parameters.put(p[0], p.length > 1 ? p[1] : Boolean.TRUE.toString());
		}
	}

	@Override
	public final String defaultSchema() {
		return supportedSchemas[0];
	}

	public String supportedSchema(String schema) {
		if (null == schema) return defaultSchema();
		for (String s : supportedSchemas)
			if (s.equalsIgnoreCase(schema)) return s;
		return null;
	}

	protected abstract C createClient(URI url) throws IOException;

	@Override
	public void close() throws IOException {}

	public final C getClient() {
		return client;
	}

	public final Map<String, String> getParameter() {
		return ImmutableMap.<String, String> copyOf(parameters);
	}

	public final String getParameter(String name) {
		return parameters.get(name);
	}
}
