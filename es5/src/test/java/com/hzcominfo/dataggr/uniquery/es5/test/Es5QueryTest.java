package com.hzcominfo.dataggr.uniquery.es5.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.Es5ConditionTransverter;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVistor;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Es5QueryTest {
    TransportClient client;

    @Before
    public void init() throws UnknownHostException {
        /*Settings settings = Settings.builder()
                .put("cluster.name", "cidev")
//                .put("client.transport.sniff", true)
//                .put("client.transport.ignore_cluster_name", true)
                .build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.11"), 39300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.12"), 39300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.13"), 39300));
*/
        Settings settings = Settings.builder()
                .put("cluster.name", "pseudo-elasticsearch")
//                .put("client.transport.sniff", true)
//                .put("client.transport.ignore_cluster_name", true)
                .build();
        client = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.16.232"), 9200));
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.16.232"), 9300));
    }

    @Test
    public void q1() {
//        String sql = "select * from uniquery.sellinfo";
        String sql = "select * from uniquery.sellinfo order by sell desc";

//        String sql = "select * from uniquery.sellinfo where name='xx'";
//        String sql = "select * from uniquery.sellinfo where name='xx' and sell = '200'";
//        String sql = "select * from uniquery.sellinfo where name='xx' or name = 'yy'";
        JsonObject json = SqlExplainer.explain(sql);
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        System.out.println("==============explainResult=======================");
        System.out.println(json);
        requestBuilder = new SearchRequestBuilderVistor(requestBuilder, json).get();
        requestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        System.out.println("==============requestBuilder=======================");
        System.out.println(requestBuilder);
        System.out.println("==============response=============================");
        SearchResponse response = requestBuilder.get();
        /*System.out.println("total: " + response.getHits().getTotalHits());
        response.getHits().forEach(hit -> {
            System.out.println(hit.getFields());
        });*/
        System.out.println(response);
    }


    public QueryBuilder q2() {
        String sql = "select * from a where NETBAR_WACODE_s = '33010635460001'";
//        String sql = "select * from a where BRAND_s = 'AppleInc'";
//        String sql = "select * from a where NETBAR_WACODE_s = '33010635460001' and MAC_s = '78-9F-70-0F-56-20'";
        JsonObject condition = SqlExplainer.explain(sql).getAsJsonObject("where");
        System.out.println("where : " + condition);
        QueryBuilder query = Es5ConditionTransverter.of(condition);
        System.out.println("=====================================");
        System.out.println(query);
        System.out.println("=====================================");
        return query;
    }


    public void createIndex() {
        IndicesAdminClient iclient = client.admin().indices();
        CreateIndexRequest ciRequest = new CreateIndexRequest();
        ciRequest.index("uniquery");
//        ciRequest.mapping()
        iclient.create(ciRequest);
    }

}
