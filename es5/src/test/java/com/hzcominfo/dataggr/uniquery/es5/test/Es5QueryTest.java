package com.hzcominfo.dataggr.uniquery.es5.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVistor;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
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
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.16.232"), 9300));
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.27.232"), 9300));
    }

    @Test
    public void query() {
//        String sql = "select * from uniquery.sell";
//        String sql = "select name, sell from uniquery.sell";
//        String sql = "select * from uniquery.sell order by name.keyword desc, sell desc";
//        String sql = "select * from uniquery.sell order by name.keyword desc, sell desc limit 4 offset 2";
//        String sql = "select * from uniquery.sell limit 4";
//        String sql = "select * from uniquery.sell offset 4";
//        String sql = "select * from uniquery.sell limit 4 offset 2";

//        String sql = "select * from uniquery.sell where name.keyword='xx'";
//        String sql = "select * from uniquery.sell where name.keyword='xx' and sell = 200";
//        String sql = "select * from uniquery.sell where name.keyword='xx' or name.keyword = 'yy'";
//        String sql = "select * from uniquery.sell where (name.keyword='xx' and sell = '200') or (name.keyword = 'yy' and sell = '250')";
//        String sql = "select * from uniquery.sell where name.keyword > 'xx'";
//        String sql = "select * from uniquery.sell where name.keyword >= 'yy'";
//        String sql = "select * from uniquery.sell where sell <= 100";
//        String sql = "select * from uniquery.sell where name.keyword < 'yy'";
//        String sql = "select * from uniquery.sell where name.keyword <= 'yy'";
//        String sql = "select * from uniquery.sell where name.keyword <> 'yy'"; // where name != 'yy'
//        String sql = "select * from uniquery.sell where name.keyword like 'y%'";
//        String sql = "select * from uniquery.sell where name.keyword like '%'";
//        String sql = "select * from uniquery.sell where name.keyword not like 'y%'";
//        String sql = "select * from uniquery.sell where remark.keyword is not null";
//        String sql = "select * from uniquery.sell where remark.keyword is null";
//        String sql = "select * from uniquery.sell where name.keyword in ('xx', 'yy')";
//        String sql = "select * from uniquery.sell where name.keyword not in ('xx', 'yy')";
//        String sql = "select * from uniquery.sell where name.keyword between 'xx' and 'yy'";
//        String sql = "select * from uniquery.sell where name.keyword not between 'xx' and 'yy'";

//        String sql = "select name, years, count(sell) as s from uniquery.sell group by name.keyword, years"; // TODO: 2017/12/4 AS for agg fun has not support well
        String sql = "select name, years as y, sum(sell), max(sell) from uniquery.sell group by name.keyword, years";

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
        System.out.println(response);
    }

    @Test
    public void multiGroupByQuery() {
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        requestBuilder.setIndices("uniquery");
        requestBuilder.setTypes("sell");
        requestBuilder.setQuery(QueryBuilders.matchAllQuery());

        AggregationBuilder aggBuilder1 = AggregationBuilders.terms("name").field("name.keyword");
        AggregationBuilder aggBuilder2 = AggregationBuilders.terms("years").field("years");
        requestBuilder.addAggregation(aggBuilder1);
        requestBuilder.addAggregation(aggBuilder2);
        SearchResponse response = requestBuilder.get();
        System.out.println(response);
    }
}
