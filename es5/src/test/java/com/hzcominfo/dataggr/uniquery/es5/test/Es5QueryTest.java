package com.hzcominfo.dataggr.uniquery.es5.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.Es5ConditionTransverter;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVistor;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
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
        Settings settings = Settings.builder()
                .put("cluster.name", "cidev")
//                .put("client.transport.sniff", true)
//                .put("client.transport.ignore_cluster_name", true)
                .build();


        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.11"), 39300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.12"), 39300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.13"), 39300));
    }
    @Test
    public void q1() {
        SearchRequestBuilder requestBuilder = client.prepareSearch("test_hzwa");
//        String sql1 = "select CAPTURE_TIME_s,LOCATION,MAC_s,BRAND_s,NETBAR_WACODE_s from test_hzwa.TEST_HZWA_WA_SOURCE_FJ_1001 where ";
        String sql1 = "select CAPTURE_TIME_s,MAC_s,BRAND_s,NETBAR_WACODE_s from test_hzwa.TEST_HZWA_WA_SOURCE_FJ_1001 ";
        String sql3 = " limit 10 offset 0";
//        String sql2 = "NETBAR_WACODE_s = '33010635460001'";
        String sql2 = "where NETBAR_WACODE_s = '33010635460001' or CAPTURE_TIME_s = '1484882948'";
//        String sql2 = "where MAC_s = '78-BD-BC-6C-FD-0C'";
//        String sql2 = "where (MAC_s = '78-BD-BC-6C-FD-0C' and CAPTURE_TIME_s = '1484882948') or (MAC_s = 'F2-F6-B0-4E-AF-40' and CAPTURE_TIME_s = '1484882948')";
//        String sql2 = " ";
        String sql = sql1 + sql2 + sql3;
        JsonObject json = SqlExplainer.explain(sql);
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
}
