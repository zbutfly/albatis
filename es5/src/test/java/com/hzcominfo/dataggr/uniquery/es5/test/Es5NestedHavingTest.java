package com.hzcominfo.dataggr.uniquery.es5.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVisitor;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
import java.util.*;
import java.util.stream.Collectors;

public class Es5NestedHavingTest {

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
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        requestBuilder
                .setIndices("tdl_uniquery_test")
                .setTypes("person")
                .setQuery(QueryBuilders.matchAllQuery())
//                .setQuery(QueryBuilders.nestedQuery("LG", QueryBuilders.matchAllQuery(), ScoreMode.Avg).innerHit(InnerHitBuilder.fromXContent("{}")))
                .setSize(0)
                .addAggregation(AggregationBuilders.nested("nested_count", "LG").subAggregation(
                        AggregationBuilders.count("cnt").field("LG.LGDM_LGMC_FORMAT_s")
                ))
                .addAggregation(AggregationBuilders.count("XM_s_CNT").field("XM_s"));

        System.out.println(requestBuilder);
        System.out.println("==============================================");
        SearchResponse response = requestBuilder.get();
        System.out.println(response);
    }


    /**
     * 测试 满足 半年内，在拱墅区住过三次以上旅馆名称里面包括“如家”两个字的旅馆
     */
    @Test
    public void q1() {
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        Map<String, String> pathsMap = new HashMap<>();
//        pathsMap.put("cnt", "nested_LG>count_LGDM_LGMC_FORMAT_s");
        pathsMap.put("cnt", "nested_LG>filter_LG>_count");
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "expression", "cnt > 3", Collections.emptyMap());
        /*QueryBuilder fq = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("LG.LGDM_LGMC_FORMAT_s", "横塘村十组旅馆"))
                .must(QueryBuilders.rangeQuery("LG.RZSJ_month_s").from("201001").includeLower(true));*/
        QueryBuilder fq = QueryBuilders.termQuery("LG.LGDM_LGMC_FORMAT_s", "横塘村十组旅馆");
        QueryBuilder query = QueryBuilders.nestedQuery("LG", fq, ScoreMode.Avg);
        requestBuilder
                .setIndices("tdl_uniquery_test")
                .setTypes("person")
                .setQuery(query)
                .setSize(0)
                .addAggregation(AggregationBuilders.terms("SFZH_s_aggrs").field("SFZH_s")/*.size(0)*/
                        .subAggregation(AggregationBuilders.nested("nested_LG", "LG")
                                .subAggregation(AggregationBuilders.filter("filter_LG", fq))
//                                .subAggregation(AggregationBuilders.count("count_LGDM_LGMC_FORMAT_s").field("LG.LGDM_LGMC_FORMAT_s"))
                        )
                        .subAggregation(PipelineAggregatorBuilders.bucketSelector("having-filter", pathsMap, script)));


        System.out.println(requestBuilder);
        System.out.println("==============================================");
        SearchResponse response = requestBuilder.get();
        System.out.println(response);
    }


    @Test
    public void h2() {
//        String sql = "select SFZH_s, XM_s, LG.LGDM_LGMC_FORMAT_s from tdl_uniquery_test.person where MODEL_NAME_s = 'gazhk_LGY_NB' limit 10";
//        String sql = "select SFZH_s, XM_s, LG.LGDM_LGMC_FORMAT_s from tdl_uniquery_test.person where LG.LGDM_LGMC_FORMAT_s = '横塘村十组旅馆' limit 10";
        String sql = "select SFZH_s, XM_s, LG.LGDM_LGMC_FORMAT_s, count(LG.LGDM_LGMC_FORMAT_s) as cnt from tdl_uniquery_test.person where LG.LGDM_LGMC_FORMAT_s = '横塘村十组旅馆' group by SFZH_s having cnt > 3";
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
    public void h3() {
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

    @Test
    public void h9() {
//        String sql = "select zjhm, count(0) as cnt from edw_lgy_nb where rzrq >= now() - 180 days and xzqh = '3309123' and lgdm_format like '%如家%' group by zjhm having cnt > 3";
//        String sql = "select zjhm, count(0) from edw_lgy_nb where rzrq >= now() - 180 days and xzqh = '3309123' and lgdm_format like '%如家%' group by zjhm having count(0) > 3";
//        String sql = "select zjhm, count(0) as cnt from edw_lgy_nb where rzrq >= 1520478480 and xzqh = '3309123' and lgdm_format like '%如家%' group by zjhm having cnt > 3";
        String sql = "select zjhm, count(0) as cnt from edw_lgy_nb where rzrq >= ? and xzqh = ? and lgdm_format like '%如家%' group by zjhm having cnt > 3 and CSRQ > 28";
        JsonObject object = SqlExplainer.explain(sql, 1520478480L, "3309123");
        System.out.println(object);
        System.out.println("hello world");
    }

}
