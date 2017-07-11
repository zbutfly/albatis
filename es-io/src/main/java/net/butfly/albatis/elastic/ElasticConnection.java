package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;

public class ElasticConnection extends NoSqlConnection<TransportClient>implements ElasticConnect {

	static {
		Connection.register("es", ElasticConnection.class);
	}

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
		return uri.getPathAt(1);
	}

	@Override
	public void mapping(Map<String, Object> mapping, String... types) {
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		if (null != getDefaultType())
			tps.add(getDefaultType());
		for (String t : tps)
			if (!client().admin().indices().putMapping(req.type(t)).actionGet().isAcknowledged())
				logger().error("Mapping failed on type [" + t + "]" + req.toString());
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
		// await();
		client().close();
	}

	protected final void await() {
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

	public static void main(String[] args) {
		String uri = "es://cominfo@hzga152:39300/";
		URISpec uriSpec = new URISpec(uri);
		
		try {
			Connection connection = Connection.connection(uriSpec, null);
			ElasticConnection ec = (ElasticConnection) connection;
			List<DiscoveryNode> nodes = ec.client().connectedNodes();
			for (DiscoveryNode node : nodes) {
				System.out.println(node.getHostAddress());
			}
			String source = "{\"hello\":\"world\"}";
			IndexResponse indexResponse = ec.client().prepareIndex("test_search", "hello", "0").setSource(source).get();
//			XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("hello", "world").endObject();
//			UpdateResponse updateResponse = ec.client().prepareUpdate("test_search", "hello", "0").setDoc(builder)
//					.get();
			System.out.println(indexResponse.getVersion());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
