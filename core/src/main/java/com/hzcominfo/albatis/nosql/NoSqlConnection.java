package com.hzcominfo.albatis.nosql;

import java.io.IOException;
import java.net.ProtocolException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Joiner;
import com.hzcominfo.albatis.search.exception.SearchAPIError;

import net.butfly.albacore.io.URISpec;

public abstract class NoSqlConnection<C> implements Connection {
	protected final String[] supportedSchemas;
	private final C client;
	protected final URISpec uri;
	protected final Properties parameters;

	protected NoSqlConnection(String connection, String... supportedSchema) throws IOException {
		super();
		supportedSchemas = null != supportedSchema ? supportedSchema : new String[0];
		uri = new URISpec(connection);
		String schema = supportedSchema(uri.getScheme());
		if (null == schema) throw new ProtocolException(uri.getScheme() + " is not supported, "//
				+ "supported list: [" + Joiner.on(',').join(supportedSchemas) + "]");
		parameters = new Properties();
		String propertiseString = uri.getQuery();
		if (propertiseString != null && !propertiseString.isEmpty()) {
			String[] propertisies = propertiseString.split("&");
			Arrays.asList(propertisies).stream().forEach(value -> {
				String[] keyValue = value.split("=");
				if (keyValue.length != 2) throw new SearchAPIError("parameter error " + Arrays.toString(keyValue));
				parameters.put(keyValue[0], keyValue[1]);
			});
		}
		client = createClient(uri);
	}

	public final Properties getParameters() {
		return parameters;
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

	protected abstract C createClient(URISpec uri) throws IOException;

	@Override
	public void close() throws IOException {}

	public final C client() {
		return client;
	}

	public final String getParameter(String name) {
		return parameters.getProperty(name);
	}

	public final String getParameter(String name, String defults) {
		return parameters.getProperty(name, defults);
	}

	public final void setParameter(String name, String value) {
		parameters.setProperty(name, value);
	}

	public final void setParameters(Map<String, String> parameters) {
		this.parameters.putAll(parameters);
	}
}
