package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.StatusLine;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RestClient;

import com.hzcominfo.albatis.nosql.DataConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.FieldDesc;

public class ElasticRestConnection extends DataConnection<RestClient> implements ElasticConnect {
	public ElasticRestConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri, 39200, "http", "https");
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
	protected RestClient initialize(URISpec uri) {
		return ElasticConnect.Builder.buildRestClient(uri);
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
	public void construct(String type, FieldDesc... fields) {
		Map<String, Object> mapping;
		MappingConstructor cstr = new MappingConstructor(Maps.of());
		mapping = cstr.construct(fields);
		logger().debug("Mapping constructing: " + mapping);
		String mappings = JsonSerder.JSON_MAPPER.ser(mapping);
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		// if (null != getDefaultType()) tps.add(getDefaultType());
		try {
			@SuppressWarnings("deprecation")
			StatusLine r = client.performRequest("PUT", getDefaultIndex() + "/_mapping/" + type, new HashMap<>(), new NStringEntity(
					mappings)).getStatusLine();
			if (200 != r.getStatusCode()) logger().error("Mapping failed on type [" + type + "]: \n\t" + r.getReasonPhrase());
		} catch (IOException ex) {
			logger().error("Mapping failed on type [" + type + "]", ex);
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
			client.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
	}
}
