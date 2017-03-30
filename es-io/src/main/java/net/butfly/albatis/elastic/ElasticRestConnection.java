package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.serder.JsonSerder;

public class ElasticRestConnection extends NoSqlConnection<RestClient> implements ElasticConnect {
	public ElasticRestConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri, u -> ElasticConnect.Builder.buildRestClient(u), 39200, "http", "https");
	}

	public ElasticRestConnection(URISpec uri) throws IOException {
		this(uri, null);
	}

	public ElasticRestConnection(String url, Map<String, String> props) throws IOException {
		this(new URISpec(url), props);
	}

	public ElasticRestConnection(String url) throws IOException {
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
		String mappings = JsonSerder.JSON_MAPPER.ser(mapping);
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		if (null != getDefaultType()) tps.add(getDefaultType());
		Response rep;
		for (String t : tps) {
			try {
				rep = client().performRequest("PUT", getDefaultIndex() + "/_mapping/" + t, new HashMap<>(), new NStringEntity(mappings));
			} catch (IOException e) {
				logger().error("Mapping failed on type [" + t + "]", e);
				break;
			}
			if (200 != rep.getStatusLine().getStatusCode()) logger().error("Mapping failed on type [" + t + "]: \n\t" + rep.getStatusLine()
					.getReasonPhrase());
		}
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
		try {
			client().close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
	}
}
