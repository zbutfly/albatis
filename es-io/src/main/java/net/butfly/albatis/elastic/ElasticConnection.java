package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.transport.TransportClient;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;

public class ElasticConnection extends NoSqlConnection<TransportClient> implements ElasticConnect {

	public ElasticConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri, u -> ElasticConnect.Builder.buildTransportClient(u, props), 39300, "es", "elasticsearch");
	}

	public ElasticConnection(URISpec uri) throws IOException {
		this(uri, null);
	}

	public ElasticConnection(String url, Map<String, String> props) throws IOException {
		this(new URISpec(url), props);
	}

	public ElasticConnection(String url) throws IOException {
		this(new URISpec(url));
	}

	@Override
	public String getDefaultIndex() {
		return uri.getPathAt(0);
	}

	@Override
	public String getDefaultType() {
		return uri.getFile();
	}

	@Override
	public void mapping(Map<String, Object> mapping, String... types) {
		logger().debug("Mapping constructing: " + mapping);
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		if (null != getDefaultType()) tps.add(getDefaultType());
		for (String t : tps)
			if (!client().admin().indices().putMapping(req.type(t)).actionGet().isAcknowledged()) logger().error("Mapping failed on type ["
					+ t + "]" + req.toString());
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
		await();
		client().close();
	}

	private void await() {
		boolean closed = false;
		logger().debug("ES connection thread pool terminating...");
		while (!closed)
			try {
				closed = client().threadPool().awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				closed = true;
			}
		logger().debug("ES connection thread pool terminated...");
	}
}
