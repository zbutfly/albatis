package net.butfly.albatis.elastic;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.utils.logger.Logger;

public class ElasticConnection extends NoSqlConnection<TransportClient> {
	private static final Logger logger = Logger.getLogger(ElasticConnection.class);
	private String defaultIndex;
	private String defaultType;

	// es://clustername@host1:port1,host2:port2.../index/type
	public ElasticConnection(String connection) throws IOException {
		super(connection, "es");
		String index = uri.getPath().split("/")[0];
		defaultIndex = "".equals(index) ? null : index;
		defaultType = parameters.get("type");

	}

	public ElasticConnection(String url, String... indexAndType) throws IOException {
		this(url);
		if (indexAndType.length > 0) defaultIndex = indexAndType[0];
		if (indexAndType.length > 1) defaultType = indexAndType[1];
		if (indexAndType.length > 2) logger.warn("only index and type parsed, other is ignore");
	}

	@Override
	protected TransportClient createClient(URI url) throws UnknownHostException {
		Settings settings = Settings.settingsBuilder().put("cluster.name", url.getAuthority())//
				.build();
		TransportClient c = TransportClient.builder().settings(settings).build();
		String[] a = url.getAuthority().split("@");
		String clusterName = a[0];
		for (String h : a[1].split(",")) {
			String[] host = h.split(":");
			int port = host.length > 1 ? 39300 : Integer.parseInt(host[1]);
			c.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host[0]), port));
		}
		return c;
	}

	public String getDefaultIndex() {
		return defaultIndex;
	}

	public String getDefaultType() {
		return defaultType;
	}

	@Override
	public void close() throws Exception {
		super.close();
		getClient().close();
	}
}
