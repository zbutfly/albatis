package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Elastic7RestHighLevelConnection extends DataConnection<RestHighLevelClient> implements Elastic7Connect {
	public Elastic7RestHighLevelConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri.extra(props), 39200, "es7:rest", "elasticsearch7:rest");
	}

	public Elastic7RestHighLevelConnection(URISpec uri) throws IOException {
		this(uri, null);
	}

	public Elastic7RestHighLevelConnection(String url, Map<String, String> props) throws IOException {
		this(new URISpec(url), props);
	}

	public Elastic7RestHighLevelConnection(String url) throws IOException {
		this(new URISpec(url));
	}

	@Override
	protected RestHighLevelClient initialize(URISpec uri) {
		return Builder.buildRestHighLevelClient(uri);
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
	public void construct(String indexType, FieldDesc... fields) {
		Map<String, Object> indexConfig = new HashMap<>();
		indexConfig.put("index/type", indexType);
		construct(indexConfig, fields);
	}

	@Override
	public void construct(Map<String, Object> indexConfig, FieldDesc... fields) {
		if (Colls.empty(indexConfig)) throw new RuntimeException("Please at least put index/type into indexConfig!");
		String alias = String.valueOf(indexConfig.get("alias"));
		indexConfig.remove("alias");
		assert null != alias;
		String table = String.valueOf(indexConfig.remove("index/type"));
		String[] tables;
		if (table.contains(".")) tables = table.split("\\.");
		else if (table.contains("/")) tables = table.split("/");
		else throw new RuntimeException("es not support other split ways!");
		String index, type;
		if (tables.length == 1) index = type = tables[0];
		else if (tables.length == 2) {
			index = tables[0];
			type = tables[1];
		} else throw new RuntimeException("Please type in correct es table format: index/type or index.type !");
		Map<String, Object> mapping = new MappingConstructor(indexConfig).construct(fields);
		logger().debug(() -> "Mapping constructing: \n\t" + JsonSerder.JSON_MAPPER.ser(mapping));
		boolean indexExists;
		try {
			indexExists = client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new RuntimeException("check index " + index + " failed", e);
		}
		if (indexExists) {
			PutMappingRequest req = new PutMappingRequest(index);
			req.source(mapping);
			AcknowledgedResponse r;
			try {
				r = client.indices().putMapping(req, RequestOptions.DEFAULT);
			} catch (IOException e) {
				throw new RuntimeException("Mapping failed on type [" + type + "]" + req.toString(), e);
			}
			if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + type + "]" + req.toString());
			else logger().info(() -> "Mapping on [" + type + "] construced sussesfully.");
			return;
		}
		CreateIndexResponse r;
		try {
			if (indexConfig.isEmpty()) r = client.indices().create(new CreateIndexRequest(index).mapping(mapping), RequestOptions.DEFAULT);
			else r = client.indices().create(new CreateIndexRequest(index).settings(indexConfig).mapping(mapping), RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new RuntimeException("Mapping failed on index [" + index + "] type [" + type + "]", e);
		}
		if (!r.isAcknowledged()) logger().error("Mapping failed on index [" + index + "] type [" + type + "]" + r.toString());
		else logger().info(() -> "Mapping on index [" + index + "] type [" + type + "] construct successfully: \n\t"
				+ JsonSerder.JSON_MAPPER.ser(mapping));
		boolean aliasExists;
		try {
			aliasExists = client.indices().existsAlias(new GetAliasesRequest(index), RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new RuntimeException("Check alias " + alias + " failed", e);
		}
		if (aliasExists) {
			IndicesAliasesRequest request = new IndicesAliasesRequest();
			AliasActions aliasAction =
			        new AliasActions(AliasActions.Type.ADD)
			        .index(index)
			        .alias(alias);
			request.addAliasAction(aliasAction);
			AcknowledgedResponse response;
			try {
				response = client.indices().updateAliases(request, RequestOptions.DEFAULT);
			} catch (IOException e) {
				throw new RuntimeException("create elastic index alias failure", e);
			}
			if (!response.isAcknowledged()) logger().error("create elastic index alias failure:" + response.toString());
			else logger().info("create elastic index alias successful");
		} else logger().info("es aliases also index duplicate names.");
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<Elastic7RestHighLevelConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public Elastic7RestHighLevelConnection connect(URISpec uriSpec) throws IOException {
			return new Elastic7RestHighLevelConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("es7:rest", "elasticsearch7:rest");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Elastic7RestOutput outputRaw(TableDesc... table) throws IOException {
		return new Elastic7RestOutput("Elastic7RestOutput", this);
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
