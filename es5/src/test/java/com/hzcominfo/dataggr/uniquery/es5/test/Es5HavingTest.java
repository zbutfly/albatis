package com.hzcominfo.dataggr.uniquery.es5.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVisitor;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Es5HavingTest {
    private TransportClient client;


    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "cidev")
                .build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.11"), 39300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.12"), 39300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.17.13"), 39300));
    }

    @After
    public void close() {
        if (null != client) client.close();
    }

    @Test
    public void h1() {
//        String sql = "select name.keyword, count(name.keyword) as cnt from uniquery.sellinfo where sell > 10 group by name.keyword having cnt > 3 and sell > 300";
        String sql = "select name.keyword, count(name.keyword) as cnt from uniquery.sellinfo where sell > 10 group by name.keyword having cnt > 3 or cnt = 4";
//        String sql = "select name.keyword, count(name.keyword) as cnt from uniquery.sellinfo where sell > 10 group by name.keyword having (a > 3 or b <= 4) and c = 7";
//        String sql = "select name.keyword, count(name.keyword) as cnt from uniquery.sellinfo where sell > 10 group by name.keyword having cnt > 3";
        JsonObject json = SqlExplainer.explain(sql);

        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        System.out.println("==============explainResult=======================");
        System.out.println(json);
        requestBuilder = new SearchRequestBuilderVisitor(requestBuilder, json).get();
        requestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        System.out.println("==============requestBuilder=======================");
        System.out.println(requestBuilder);

        System.out.println("==============response=============================");
        SearchResponse response = requestBuilder.get();
        System.out.println(response);
    }

    @Test
    public void h2() {
        List<String> havingFields = new ArrayList<>();
        havingFields.add("cnt");

        Map<String, String> bsPathMap = havingFields.stream().collect(Collectors.toMap(s -> s, s -> s));
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "expression", "cnt >= 2 and cnt < 4", Collections.emptyMap());
        BucketSelectorPipelineAggregationBuilder builder = PipelineAggregatorBuilders
                .bucketSelector("having-filter", bsPathMap, script);
        SearchRequestBuilder srBuilder = client.prepareSearch("");
        srBuilder.setIndices("uniquery");
        srBuilder.setTypes("sellinfo");
        srBuilder.setSize(0);
        srBuilder.setFetchSource(new String[] {"name"}, new String[] {});
        srBuilder.setQuery(QueryBuilders.rangeQuery("sell").from(10).includeLower(true));
        srBuilder.addAggregation(AggregationBuilders.terms("name_aggrs").field("name.keyword").subAggregation(
                AggregationBuilders.count("cnt").field("name.keyword")
        ).subAggregation(builder));
        System.out.println(srBuilder);

        SearchResponse response = srBuilder.get();
        System.out.println("===============================================");
        System.out.println(response);
    }

    @Deprecated
    @Test
    public void h3() {

    }



    @Test
    public void h9() {
//        String sql = "select zjhm, count(0) as cnt from edw_lgy_nb where rzrq >= now() - 180 days and xzqh = '3309123' and lgdm_format like '%如家%' group by zjhm having cnt > 3";
//        String sql = "select zjhm, count(0) from edw_lgy_nb where rzrq >= now() - 180 days and xzqh = '3309123' and lgdm_format like '%如家%' group by zjhm having count(0) > 3";
//        String sql = "select zjhm, count(0) as cnt from edw_lgy_nb where rzrq >= 1520478480 and xzqh = '3309123' and lgdm_format like '%如家%' group by zjhm having cnt > 3";
        String sql = "select zjhm, count(0) as cnt from edw_lgy_nb where rzrq >= ? and xzqh = ? and lgdm_format like '%如家%' group by zjhm having cnt > 3 and CSRQ > 28";
        JsonObject object = SqlExplainer.explain(sql, 1520478480L, "3309123");
        System.out.println(object);
    }

}
