package com.hzcominfo.albatis.nosql;

import java.io.IOException;
import java.net.ProtocolException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import com.google.common.base.Joiner;
import com.hzcominfo.albatis.search.exception.SearchAPIError;

import net.butfly.albacore.io.URISpec;

public abstract class NoSqlConnection<C> implements Connection {
	protected final String[] supportedSchemas;
	private final C client;
	protected final URISpec uri;
	protected final Properties parameters;

	protected NoSqlConnection(URISpec uri, Function<URISpec, C> client, String... supportedSchema) throws IOException {
		super();
		supportedSchemas = null != supportedSchema ? supportedSchema : new String[0];
		this.uri = uri;
		String schema = supportedSchema(uri.getScheme());
		if (null == schema) throw new ProtocolException(uri.getScheme() + " is not supported, "//
				+ "supported list: [" + Joiner.on(',').join(supportedSchemas) + "]");
		this.client = client.apply(uri);

		parameters = new Properties();
		String qstr = uri.getQuery();
		if (qstr != null && !qstr.isEmpty()) {
			String[] propertisies = qstr.split("&");
			Arrays.asList(propertisies).stream().forEach(value -> {
				String[] keyValue = value.split("=", 2);
				if (keyValue.length != 2) throw new SearchAPIError("parameter error " + Arrays.toString(keyValue));
				parameters.put(keyValue[0], keyValue[1]);
			});
		}
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

	@Override
	public URISpec getURI() {
		return uri;
	}
}
