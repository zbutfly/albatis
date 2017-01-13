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
	protected ElasticURI elasticURI;

	public ElasticConnection(String connection) throws IOException {
		super(connection, "elasticsearch");
	}

	public ElasticConnection(String url, String... indexAndType) throws IOException {
		this(url);
		if (indexAndType.length > 2) logger.warn("only index and type parsed, other is ignore");
	}

	@Override
	protected TransportClient createClient(URI url) throws UnknownHostException {
		elasticURI = new ElasticURI(uri);
		Settings settings = elasticURI.getSettings(getParameters());
		TransportClient c = TransportClient.builder().settings(settings).build();
		String hosts = elasticURI.getHost();
		for (String h : hosts.split(",")) {
			String[] host = h.split(":");
			int port = host.length > 1 ? Integer.parseInt(host[1]) : 39300;
			c.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host[0]), port));
		}
		return c;
	}

	public String getDefaultIndex() {
		return elasticURI.getIndex();
	}

	public String getDefaultType() {
		return elasticURI.getType();
	}

	@Override
	public void close() throws IOException {
		super.close();
		getClient().close();
	}
}
