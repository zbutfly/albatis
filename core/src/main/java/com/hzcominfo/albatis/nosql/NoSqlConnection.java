package com.hzcominfo.albatis.nosql;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.ProtocolException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.hzcominfo.albatis.search.exception.SearchAPIError;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Loggable;

public abstract class NoSqlConnection<C> implements Connection, Loggable {
	protected final String[] supportedSchemas;
	private final C client;
	protected final URISpec uri;
	protected final Properties parameters;

	protected NoSqlConnection(URISpec uri, Function<URISpec, C> client, int defaultPort, String... supportedSchema)
			throws IOException {
		super();
		supportedSchemas = null != supportedSchema ? supportedSchema : new String[0];
		this.uri = uri;
		if (this.uri.getDefaultPort() < 0)
			this.uri.setDefaultPort(defaultPort);
		String schema = supportedSchema(uri.getScheme());
		if (null == schema)
			throw new ProtocolException(uri.getScheme() + " is not supported, "//
					+ "supported list: [" + Joiner.on(',').join(supportedSchemas) + "]");
		this.client = client.apply(uri);

		parameters = new Properties();
		String qstr = uri.getQuery();
		if (qstr != null && !qstr.isEmpty()) {
			String[] propertisies = qstr.split("&");
			Stream.of(propertisies).forEach(value -> {
				String[] keyValue = value.split("=", 2);
				if (keyValue.length != 2)
					throw new SearchAPIError("parameter error " + Arrays.toString(keyValue));
				parameters.put(keyValue[0], keyValue[1]);
			});
		}
	}

	protected NoSqlConnection(URISpec uri, Function<URISpec, C> client, String... supportedSchema) throws IOException {
		this(uri, client, -1, supportedSchema);
	}

	public final int getBatchSize() {
		String b = parameters.getProperty(PARAM_KEY_BATCH);
		return null != b ? Integer.parseInt(b) : DEFAULT_BATCH_SIZE;
	}

	public final Properties getParameters() {
		return parameters;
	}

	@Override
	public final String defaultSchema() {
		return supportedSchemas[0];
	}

	public String supportedSchema(String schema) {
		if (null == schema)
			return defaultSchema();
		for (String s : supportedSchemas)
			if (s.equalsIgnoreCase(schema))
				return s;
		return null;
	}

	@Override
	public void close() throws IOException {
	}

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

	@Override
	public Connection connection(String url) throws Exception {
		URISpec uriSpec = new URISpec(url);
		return connection(uriSpec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Connection connection(URISpec uriSpec) throws Exception {
		Class clazz;
		Connection connection = null;
		if (uriSpec.getScheme().equalsIgnoreCase("mongodb")) {
			clazz = Connection.getRegisterInfo("mongodb");
			Constructor con = clazz.getConstructor(new Class[] { URISpec.class });
			connection = (Connection) con.newInstance(uriSpec);
		}
		return connection;
	}
}
