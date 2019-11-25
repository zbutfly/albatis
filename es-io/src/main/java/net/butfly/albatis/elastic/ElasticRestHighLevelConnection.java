package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.serder.json.Jsons;
import net.butfly.albatis.ddl.Qualifier;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;

import static net.butfly.albacore.utils.collection.Colls.empty;

public class ElasticRestHighLevelConnection extends DataConnection<RestHighLevelClient> implements ElasticConnect {
	public static final String KERBEROS_CONF_PATH = Configs.gets("albatis.es.kerberos.conf.path");
	private Kerberos kerberos;

	public ElasticRestHighLevelConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri, 39200, "es:rest", "elasticsearch:rest");
	}

	public ElasticRestHighLevelConnection(URISpec uri) throws IOException {
		this(uri, null);
	}

	public ElasticRestHighLevelConnection(String url, Map<String, String> props) throws IOException {
		this(new URISpec(url), props);
	}

	public ElasticRestHighLevelConnection(String url) throws IOException {
		this(new URISpec(url));
	}

	@Override
	protected RestHighLevelClient initialize(URISpec uri) {
		kerberos = new Kerberos(KERBEROS_CONF_PATH);
		try {
			kerberos.load();
		} catch (IOException e) {
			logger().info("kerberos disabled", e);
		}
		if (kerberos.kerberosEnable()) {
			uri.reauth(kerberos.getUser());
			return Builder.buildRestHighLevelClientWithKerberos(uri);
		}
		return ElasticConnect.Builder.buildRestHighLevelClient(uri);
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
	public void construct(Qualifier qualifier, FieldDesc... fields) {
		Map<String, Object> indexConfig = new HashMap<>();
		indexConfig.put("index/type", qualifier.name);
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
			indexExists = client.indices().exists(new GetIndexRequest().indices(index), RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new RuntimeException("check index " + index + " failed", e);
		}
		if (indexExists) {
			PutMappingRequest req = new PutMappingRequest(index);
			req.source(mapping);
			AcknowledgedResponse r;
			try {
				r = client.indices().putMapping(req.type(type), RequestOptions.DEFAULT);
			} catch (IOException e) {
				throw new RuntimeException("Mapping failed on type [" + type + "]" + req.toString(), e);
			}
			if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + type + "]" + req.toString());
			else logger().info(() -> "Mapping on [" + type + "] construced sussesfully.");
			return;
		}
		CreateIndexResponse r;
		try {
			if (indexConfig.isEmpty()) r = client.indices().create(new CreateIndexRequest(index).mapping(type, mapping), RequestOptions.DEFAULT);
			else r = client.indices().create(new CreateIndexRequest(index).settings(indexConfig).mapping(type, mapping), RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new RuntimeException("Mapping failed on index [" + index + "] type [" + type + "]", e);
		}
		if (!r.isAcknowledged()) logger().error("Mapping failed on index [" + index + "] type [" + type + "]" + r.toString());
		else logger().info(() -> "Mapping on index [" + index + "] type [" + type + "] construct successfully: \n\t"
				+ JsonSerder.JSON_MAPPER.ser(mapping));
		boolean aliasExists;
		try {
			aliasExists = client.indices().exists(new GetIndexRequest().indices(index), RequestOptions.DEFAULT);
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

    @Override
	public void construct(Map<String, Object> mapping, String indexAndType) {
        String[] it = indexAndType.split("/", 2);
        String[] its = 2 == it.length ? it : new String[]{it[0], null};
        if (empty(its[0])) its[0] = getDefaultIndex();
        if (empty(its[1])) its[1] = getDefaultType();
        if (empty(its[1])) its[1] = "_doc";
        logger().debug("Mapping constructing on " + String.join("/", its) + ": \n\t" + Jsons.pretty(mapping));

        PutMappingRequest req = new PutMappingRequest(its[0]);
        req.source(mapping);
        AcknowledgedResponse r;
        try {
            r = client.indices().putMapping(req.type(it[1]), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Mapping failed on type [" + it[1] + "]" + req.toString(), e);
        }
        if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + its[1] + "]" + req.toString());
        else logger().info(() -> "Mapping on " + Arrays.toString(its) + " construced sussesfully.");
    }


    public static class Driver implements net.butfly.albatis.Connection.Driver<ElasticRestHighLevelConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public ElasticRestHighLevelConnection connect(URISpec uriSpec) throws IOException {
			return new ElasticRestHighLevelConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("es:rest", "elasticsearch:rest");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public ElasticRestOutput outputRaw(TableDesc... table) throws IOException {
		return new ElasticRestOutput("ElasticRestOutput", this);
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
