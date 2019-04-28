package net.butfly.albatis;

import static net.butfly.albatis.io.IOProps.BATCH_SIZE;
import static net.butfly.albatis.io.IOProps.propI;

import java.io.IOException;
import java.net.ProtocolException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Loggable;

public abstract class DataConnection<C> implements Connection, Loggable {
	protected final String[] supportedSchemas;
	public final C client;
	protected final URISpec uri;
	protected final Properties parameters;

	public DataConnection() {
		// Required for JAVA SPI
		uri = null;
		supportedSchemas = null;
		client = null;
		parameters = null;
	}

	protected DataConnection(URISpec uri, int defaultPort, String... supportedSchema) throws IOException {
		super();
		supportedSchemas = null != supportedSchema ? supportedSchema : new String[0];
		this.uri = uri;
		if (this.uri.getDefaultPort() < 0) this.uri.setDefaultPort(defaultPort);
		String schema = supportedSchema(uri.getSchema());
		if (null == schema) throw new ProtocolException(uri.getSchema() + " is not supported, "//
				+ "supported list: [" + String.join(",", supportedSchemas) + "]");
		this.client = initialize(uri);// client.apply(uri);

		parameters = new Properties();
		String qstr = uri.getQuery();
		if (qstr != null && !qstr.isEmpty()) {
			Arrays.asList(qstr.split("&")).forEach(value -> {
				String[] keyValue = value.split("=", 2);
				if (keyValue.length != 2) throw new IllegalArgumentException("parameter error " + Arrays.toString(keyValue));
				parameters.put(keyValue[0], keyValue[1]);
			});
		}
	}

	protected C initialize(URISpec uri) {
		return null;
	}

	protected DataConnection(URISpec uri, String... supportedSchema) throws IOException {
		this(uri, -1, supportedSchema);
	}

	public final int batchSize() {
		return propI(this, BATCH_SIZE, defBatchSize());
	}

	private int defBatchSize() {
		String b = parameters.getProperty(PARAM_KEY_BATCH);
		return null != b ? Integer.parseInt(b) : DEFAULT_BATCH_SIZE;
	}

	public final Properties getParameters() { return parameters; }

	@Override
	public final String defaultSchema() {
		return supportedSchemas[0];
	}

	public String supportedSchema(String schema) {
		if (null == schema) return defaultSchema();
		for (String s : supportedSchemas) if (schema.toLowerCase().startsWith(s.toLowerCase())) return s;
		return defaultSchema();
	}

	@Override
	public void close() throws IOException {}

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
	public URISpec uri() {
		return uri;
	}

	@Override
	protected void finalize() throws Throwable {
		close();
	}
}
