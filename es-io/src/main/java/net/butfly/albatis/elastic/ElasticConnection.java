package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.TransportClient;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;

public class ElasticConnection extends DataConnection<TransportClient> implements ElasticConnect {
	public ElasticConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri.extra(props), 39300, "es", "elastic", "elasticsearch");
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
	protected TransportClient initialize(URISpec uri) {
		return ElasticConnect.Builder.buildTransportClient(uri, uri.extras);
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
	public void construct(String indexType, FieldDesc... fields) {
		Map<String, Object> indexConfig = new HashMap<>();
		indexConfig.put("index/type", indexType);
		construct(indexConfig, fields);
	}

	/**
	 * Please at least put index/type into indexConfig, that is to say indexConfig could not be null. Except index/type, please make sure
	 * the index configure to be according to es official config (e.g. indexConfig.put("index.number_of_shards", 3) ,
	 * indexConfig.put("index.number_of_replicas", 2)).
	 */
	@Override
	public void construct(Map<String, Object> indexConfig, FieldDesc... fields) {
		if (Colls.empty(indexConfig)) throw new RuntimeException("Please at least put index/type into indexConfig!");
		String alias = String.valueOf(indexConfig.get("alias"));
		indexConfig.remove("alias");
		assert null != alias;
		String[] table = String.valueOf(indexConfig.remove("index/type")).split("/");
		String index, type;
		if (table.length == 2) {
			index = table[0];
			type = table[1];
		} else if (table.length == 1) {
			type = index = table[0];
		} else throw new RuntimeException("Please type in corrent es table format: index/type !");
		Map<String, Object> mapping = new MappingConstructor(indexConfig).construct(fields);
		logger().debug(() -> "Mapping constructing: \n\t" + JsonSerder.JSON_MAPPER.ser(mapping));
		if (client.admin().indices().prepareExists(index).execute().actionGet().isExists()) {
			PutMappingRequest req = new PutMappingRequest(index);
			req.source(mapping);
			AcknowledgedResponse r = client.admin().indices().putMapping(req.type(type)).actionGet();
			if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + type + "]" + req.toString());
			else logger().info(() -> "Mapping on [" + type + "] construced sussesfully.");
			return;
		}
		CreateIndexResponse r;
		if (indexConfig.isEmpty()) r = client.admin().indices().prepareCreate(index).addMapping(type, mapping).get();
		else r = client.admin().indices().prepareCreate(index).setSettings(indexConfig).addMapping(type, mapping).get();
		if (!r.isAcknowledged()) logger().error("Mapping failed on index [" + index + "] type [" + type + "]" + r.toString());
		else logger().info(() -> "Mapping on index [" + index + "] type [" + type + "] construct successfully: \n\t"
				+ JsonSerder.JSON_MAPPER.ser(mapping));
		IndicesExistsRequest indexExists = new IndicesExistsRequest(index);
		IndicesExistsRequest aliasExists = new IndicesExistsRequest(alias);
		if (client.admin().indices().exists(indexExists).actionGet().isExists() && !client.admin().indices().exists(aliasExists).actionGet()
				.isExists()) {
			AcknowledgedResponse response = client.admin().indices().prepareAliases().addAlias(index, alias).execute().actionGet();
			if (!response.isAcknowledged()) logger().error("create elastic index alias failure:" + response.toString());
			else logger().info("create elastic index alias successful");
		} else logger().info("es aliases also index duplicate names.");
	}

	@Override
	public boolean judge(String index, String type) {
		boolean exists = false;
		try (ElasticConnection elasticConnection = new ElasticConnection(new URISpec(uri.toString()))) {
			IndicesExistsRequest existsRequest = new IndicesExistsRequest(index);
			exists = elasticConnection.client.admin().indices().exists(existsRequest).actionGet().isExists();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return exists;
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
		// await();
		client.close();
	}

	protected final void await() {
		boolean closed = false;
		logger().debug("ES connection thread pool terminating...");
		while (!closed)
			try {
				closed = client.threadPool().awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				closed = true;
			}
		logger().debug("ES connection thread pool terminated...");
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<ElasticConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public ElasticConnection connect(URISpec uriSpec) throws IOException {
			return new ElasticConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("es");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public ElasticOutput outputRaw(TableDesc... table) throws IOException {
		return new ElasticOutput("ElasticOutput", this);
	}

	@Deprecated
	public void construct(Map<String, Object> mapping, String... types) {
		logger().debug("Mapping constructing: \n\t" + JsonSerder.JSON_MAPPER.ser(mapping));
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		if (null != getDefaultType()) tps.add(getDefaultType());
		for (String t : tps) {
			AcknowledgedResponse r = client.admin().indices().putMapping(req.type(t)).actionGet();
			if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + t + "]" + req.toString());
			else logger().info(() -> "Mapping on [" + t + "] construced sussesfully.");
		}
	}
}
