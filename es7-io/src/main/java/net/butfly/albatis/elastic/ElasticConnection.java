package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElasticConnection extends DataConnection<TransportClient> implements ElasticConnect {

    private static final String DEFAULT_TYPE = "_doc";

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
        return Builder.buildTransportClient(uri, uri.extras);
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
    public void construct(Qualifier qualifier, FieldDesc... fields) {
        Map<String, Object> indexConfig = new HashMap<>();
        indexConfig.put("index", qualifier.name);
        construct(indexConfig, fields);
    }

    @Override
    public void construct(Map<String, Object> indexConfig, FieldDesc... fields) {
        if (Colls.empty(indexConfig)) throw new RuntimeException("Please at least put index into indexConfig!");
        String alias = String.valueOf(indexConfig.get("alias"));
        indexConfig.remove("alias");
        assert null != alias;
        String index = String.valueOf(indexConfig.remove("index"));
        Map<String, Object> mapping = new MappingConstructor(indexConfig).construct(fields);
        logger().debug(() -> "Mapping constructing: \n\t" + JsonSerder.JSON_MAPPER.ser(mapping));
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists()) {
            PutMappingRequest req = new PutMappingRequest(index);
            req.source(mapping);
            req.type(DEFAULT_TYPE);
            AcknowledgedResponse r = client.admin().indices().putMapping(req).actionGet();
            if (!r.isAcknowledged()) logger().error("Mapping failed " + req.toString());
            else logger().info(() -> "Mapping construced sussesfully.");
            return;
        }
        CreateIndexResponse r;
        if (indexConfig.isEmpty()) r = client.admin().indices().prepareCreate(index).addMapping(DEFAULT_TYPE, mapping).get();
        else r = client.admin().indices().prepareCreate(index).setSettings(indexConfig).addMapping(DEFAULT_TYPE, mapping).get();
        if (!r.isAcknowledged()) logger().error("Mapping failed on index [" + index + "] " + r.toString());
        else logger().info(() -> "Mapping on index [" + index + "] construct successfully: \n\t"
                + JsonSerder.JSON_MAPPER.ser(mapping));
        IndicesExistsRequest indexExists = new IndicesExistsRequest(index);
        if (client.admin().indices().exists(indexExists).actionGet().isExists()) {
            AcknowledgedResponse response = client.admin().indices().prepareAliases().addAlias(index, alias).execute().actionGet();
            if (!response.isAcknowledged()) logger().error("create elastic index alias failure:" + response.toString());
            else logger().info("create elastic index alias successful");
        } else logger().info("es aliases also index duplicate names.");
    }

    public void construct(Map<String, Object> mapping, String index) {
        PutMappingRequest req = new PutMappingRequest(index);
        req.source(mapping);
        req.type(DEFAULT_TYPE);
        AcknowledgedResponse r = client.admin().indices().putMapping(req).actionGet();
        if (!r.isAcknowledged()) logger().error("Mapping failed " + req.toString());
        else logger().info(() -> "Mapping on " + index + " construced sussesfully.");
    }

    @Override
    public boolean judge(String index) {
        boolean exists = false;
        try (ElasticConnection elastic7Connection = new ElasticConnection(new URISpec(uri.toString()))) {
            IndicesExistsRequest existsRequest = new IndicesExistsRequest(index);
            exists = elastic7Connection.client.admin().indices().exists(existsRequest).actionGet().isExists();
        } catch (IOException e) {
            logger().error("es judge table isExists error", e);
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
}
