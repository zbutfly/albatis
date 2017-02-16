package net.butfly.albatis.elastic;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.logger.Logger;

public class ElasticConnection extends NoSqlConnection<TransportClient> {
	private static final Logger logger = Logger.getLogger(ElasticConnection.class);

	public ElasticConnection(String connection, Map<String, String> props) throws IOException {
		this(new URISpec(connection), props);
	}

	public ElasticConnection(String url) throws IOException {
		this(url, (Map<String, String>) null);
	}

	public ElasticConnection(URISpec esURI) throws IOException {
		this(esURI, null);
	}

	public ElasticConnection(URISpec esURI, Map<String, String> props) throws IOException {
		super(esURI, uri -> {
			Settings.Builder settings = Settings.settingsBuilder();
			if (null != props && !props.isEmpty()) settings.put(uri.getParameters());
			settings.put(uri.getParameters());
			settings.put("client.transport.ignore_cluster_name", true);
			TransportClient c = TransportClient.builder().settings(settings).build();
			for (Pair<String, Integer> h : uri.getHosts()) {
				int port = h.v2() == null ? 39300 : h.v2();
				try {
					c.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(h.v1()), port));
				} catch (UnknownHostException e) {
					throw new RuntimeException(e);
				}
			}
			return c;
		}, "es", "elasticsearch");
	}

	public String getDefaultIndex() {
		return uri.getPathAt(0);
	}

	public String getDefaultType() {
		return uri.getPathAt(1);
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
		client().close();
	}
}
