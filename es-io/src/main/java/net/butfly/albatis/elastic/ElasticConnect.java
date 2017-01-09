package net.butfly.albatis.elastic;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import net.butfly.albacore.utils.logger.Logger;

public final class ElasticConnect implements AutoCloseable {
	private static final Logger logger = Logger.getLogger(ElasticConnect.class);
	private final TransportClient client;
	private String defaultIndex;
	private String defaultType;
	private final Map<String, String> parameters;

	public ElasticConnect(String connection) throws IOException {
		super();
		URL url = new URL(connection);
		if (null != url.getProtocol() && !"es".equalsIgnoreCase(url.getProtocol())) throw new ProtocolException(url.getProtocol()
				+ " is not supported");

		String index = url.getPath().split("/")[0];
		defaultIndex = "".equals(index) ? null : index;
		parameters = new HashMap<>();
		for (String param : url.getQuery().split("&")) {
			String[] p = param.split("=", 2);
			parameters.put(p[0], p.length > 1 ? p[1] : Boolean.TRUE.toString());
		}
		defaultType = parameters.get("type");

		Settings settings = Settings.settingsBuilder().put("cluster.name", url.getAuthority())//
				.build();
		client = TransportClient.builder().settings(settings).build();
		for (String h : url.getHost().split(",")) {
			String[] host = h.split(":");
			int port = host.length > 1 ? 39300 : Integer.parseInt(host[1]);
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host[0]), port));
		}
	}

	public ElasticConnect(String url, String... indexAndType) throws IOException {
		this(url);
		if (indexAndType.length > 0) defaultIndex = indexAndType[0];
		if (indexAndType.length > 1) defaultType = indexAndType[1];
		if (indexAndType.length > 2) logger.warn("only index and type parsed, other is ignore");
	}

	public TransportClient getClient() {
		return client;
	}

	public String getDefaultIndex() {
		return defaultIndex;
	}

	public String getDefaultType() {
		return defaultType;
	}

	public Map<String, String> getParameter() {
		return parameters;
	}

	public String getParameter(String name) {
		return parameters.get(name);
	}

	@Override
	public void close() {
		client.close();
	}
}
