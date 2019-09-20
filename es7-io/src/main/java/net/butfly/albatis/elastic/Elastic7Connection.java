package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.serder.json.Jsons;
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

import static net.butfly.albacore.utils.collection.Colls.empty;

public class Elastic7Connection extends DataConnection<TransportClient> implements Elastic7Connect{

    public Elastic7Connection(URISpec uri, Map<String, String> props) throws IOException {
        super(uri.extra(props), 39300, "es7", "elastic7", "elasticsearch7");
    }

    public Elastic7Connection(URISpec uri) throws IOException {
        this(uri, null);
    }

    public Elastic7Connection(String url, Map<String, String> props) throws IOException {
        this(new URISpec(url), props);
    }

    public Elastic7Connection(String url) throws IOException {
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
        Map<String, Object> mapping = new Elastic7MappingConstructor(indexConfig).construct(fields);
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
        if (client.admin().indices().exists(indexExists).actionGet().isExists()) {
            AcknowledgedResponse response = client.admin().indices().prepareAliases().addAlias(index, alias).execute().actionGet();
            if (!response.isAcknowledged()) logger().error("create elastic index alias failure:" + response.toString());
            else logger().info("create elastic index alias successful");
        } else logger().info("es aliases also index duplicate names.");
    }

    @Override
    public boolean judge(String table) {
        boolean exists = false;
        String[] tables;
        if (table.contains(".")) tables = table.split("\\.");
        else if (table.contains("/")) tables = table.split("/");
        else throw new RuntimeException("es not support other split ways!");
        String index;
        if (tables.length == 1) index = tables[0];
        else if (tables.length == 2) index = tables[0];
        else throw new RuntimeException("Please type in correct es table format: index/type or index.type !");
        try (Elastic7Connection elastic7Connection = new Elastic7Connection(new URISpec(uri.toString()))) {
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

    public static class Driver implements net.butfly.albatis.Connection.Driver<Elastic7Connection> {
        static {
            DriverManager.register(new Driver());
        }

        @Override
        public Elastic7Connection connect(URISpec uriSpec) throws IOException {
            return new Elastic7Connection(uriSpec);
        }

        @Override
        public List<String> schemas() {
            return Colls.list("es7");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Elastic7Output outputRaw(TableDesc... table) throws IOException {
        return new Elastic7Output("Elastic7Output", this);
    }

    public void construct(Map<String, Object> mapping, String indexAndType) {
        String[] it = indexAndType.split("/", 2);
        String[] its = 2 == it.length ? it : new String[] { it[0], null };
        if (empty(its[0])) its[0] = getDefaultIndex();
        if (empty(its[1])) its[1] = getDefaultType();
        if (empty(its[1])) its[1] = "_doc";
        logger().debug("Mapping constructing on " + String.join("/", its) + ": \n\t" + Jsons.pretty(mapping));

        PutMappingRequest req = new PutMappingRequest(its[0]);
        req.source(mapping);
        AcknowledgedResponse r = client.admin().indices().putMapping(req.type(its[1])).actionGet();
        if (!r.isAcknowledged()) logger().error("Mapping failed on type [" + its[1] + "]" + req.toString());
        else logger().info(() -> "Mapping on " + its + " construced sussesfully.");
    }
}
