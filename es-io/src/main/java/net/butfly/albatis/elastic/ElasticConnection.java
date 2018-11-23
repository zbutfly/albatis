package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class ElasticConnection extends NoSqlConnection<TransportClient> implements ElasticConnect {
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
		String[] table = indexType.split("/");
		String index, type;
		if (table.length == 2) {index = table[0]; type = table[1];}
		else if (table.length == 1) {type = index = table[0];}
		else throw new RuntimeException("Please type in corrent es table format: index/type !");
		
		MappingConstructor cstr = new MappingConstructor();
		Map<String, Object> mapping = cstr.construct(fields);
		logger().debug("Mapping constructing: " + mapping);
		CreateIndexResponse r = client.admin().indices().prepareCreate(index).addMapping(type, mapping).get();
		if (!r.isAcknowledged()) logger().error("Mapping failed on index [" + index + "] type [" + type + "]" + r.toString());
		else logger().info(() -> "Mapping on index [" + index + "] type [" + type + "] construced sussesfully: \n\t" + JsonSerder.JSON_MAPPER.ser(mapping));
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

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<ElasticConnection> {
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

	@Override
	public Input<Rmap> input(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ElasticOutput output(TableDesc... table) throws IOException {
		return new ElasticOutput("ElasticOutput", this);
	}

	@Deprecated
	public void construct(Map<String, Object> mapping, String... types) {
		logger().debug("Mapping constructing: " + mapping);
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		if (null != getDefaultType()) tps.add(getDefaultType());
		for (String t : tps) {
			PutMappingResponse r = client.admin().indices().putMapping(req.type(t)).actionGet();
			if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + t + "]" + req.toString());
			else logger().info(() -> "Mapping on [" + t + "] construced sussesfully: \n\t" + JsonSerder.JSON_MAPPER.ser(mapping));
		}
	}
}
