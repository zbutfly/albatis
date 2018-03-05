package com.hzcominfo.dataggr.uniquery.es5.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVisitor;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Es5NestedQueryHZWA {

    private static String index = "subject_person", type = "person";

    private TransportClient client;
    private IndicesAdminClient indexAdminClient;

    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder()
//                .put("cluster.name", "pseudo-elasticsearch")
        		.put("cluster.name", "cidev")	
                .build();
        client = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.16.232"), 9300));
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.27.232"), 9300));
        indexAdminClient = client.admin().indices();
    }

    @Test
    public void prepare_test() throws IOException {

        if (indexAdminClient.exists(new IndicesExistsRequest(index)).actionGet().isExists()) {
            indexAdminClient.prepareDelete(index).get();
        }
        indexAdminClient.prepareCreate(index).get();
        PutMappingRequestBuilder mappingRequestBuilder = indexAdminClient.preparePutMapping(index);
        mappingRequestBuilder.setType(type);
        String mapping = Files.lines(Paths.get("src/test/resources/person.json")).findFirst().get();
        mappingRequestBuilder.setSource(mapping, XContentType.JSON);
        mappingRequestBuilder.get();

        // write data
        Files.lines(Paths.get("src/test/resources/person.json")).skip(1).forEach(str -> {
            IndexRequestBuilder builder = client.prepareIndex(index, type).setSource(str, XContentType.JSON);
            builder.get();
        });
    }

    @Test
    public void t1() {
        boolean exist = indexAdminClient.exists(new IndicesExistsRequest(index)).actionGet().isExists();
        System.out.println("exist: " + exist);
    }

    @Test
    public void t2() {
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        requestBuilder.setIndices(index);
        requestBuilder.setTypes(type);
//        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder query = QueryBuilders.nestedQuery("LG", QueryBuilders.termQuery("LG.LGDM_s", "3301060608"), ScoreMode.Avg);
        requestBuilder.setQuery(query);
        System.out.println(requestBuilder);
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++");
        SearchResponse response = requestBuilder.get();
        System.out.println("total: " + response.getHits().getTotalHits());
        response.getHits().forEach(hit -> System.out.println(hit.getSourceAsString()));
    }

    @Test
    public void n1() {
//        String sql = "select * from " + index + "." + type + " limit 10";
        /** nested in fields */
//        String sql = "select XM_s from " + index + "." + type + " limit 10";
//        String sql = "select keyword(XM_s) from " + index + "." + type + " limit 10";
//        String sql = "select XM_s, keyword(TL.TO_STATION_NAME_s) from " + index + "." + type + " limit 10";
        /** nested in condition */
        /** unary condition */
//        String sql = "select XM_s, keyword(LG.LGDM_s) from " + index + "." + type + " where LG.LGDM_s is not null limit 10"; // TODO: 2018/1/12 pass!
        /** binary condition */
//        String sql = "select XM_s, keyword(LG.LGDM_s) from " + index + "." + type + " where LG.LGDM_s = '3301060608' limit 10";
//        String sql = "select XM_s, keyword(LG.LKZT_FORMAT_s) from " + index + "." + type + " where keyword(LG.LKZT_FORMAT_s) = '离店' limit 10"; // TODO: 2018/1/12 no result found!
//        String sql = "select XM_s, keyword(LG.LGDM_s) from " + index + "." + type + " where keyword(LG.LGDM_s) = '3301060608' limit 10"; // TODO: 2018/1/12 no result found!
//        String sql = "select XM_s, keyword(LG.LGDM_s) from " + index + "." + type + " where LG.LGDM_s = '3301060608' limit 10";  // TODO: 2018/1/12 pass !!
//        String sql = "select XM_s, keyword(LG.LGDM_s) from " + index + "." + type + " where LG.LGDM_s like '330106%' limit 10"; // TODO: 2018/1/12 pass!
//        String sql = "select XM_s, keyword(LG.LGDM_s), LG.LDSJ_year_s from " + index + "." + type + " where LG.LDSJ_year_s >= '2013' limit 10"; // TODO: 2018/1/12 pass!
//        String sql = "select XM_s, keyword(LG.LGDM_s), LG.LDSJ_year_s from " + index + "." + type + " where LG.LDSJ_year_s > '2013' limit 10"; // TODO: 2018/1/12 pass!
//        String sql = "select XM_s, keyword(LG.LGDM_s), LG.LDSJ_year_s from " + index + "." + type + " where LG.LDSJ_year_s < '2009' limit 10"; // TODO: 2018/1/12 pass!
        /** ternary condition */
//        String sql = "select XM_s, keyword(LG.LGDM_s), LG.LDSJ_year_s from " + index + "." + type + " where LG.LDSJ_year_s between '2001' and '2002' limit 10"; // TODO: 2018/1/12 pass!
        /** multiple condition */
//        String sql = "select XM_s, keyword(LG.LGDM_s), LG.LDSJ_year_s from " + index + "." + type + " where LG.LDSJ_year_s in ('2011', '2012') limit 10"; // TODO: 2018/1/12 pass!
//        String sql = "select XM_s, keyword(LG.LGDM_s), LG.LDSJ_year_s from " + index + "." + type + " where LG.LDSJ_year_s in ('1999', '2000', '2001', '2002') limit 10"; // TODO: 2018/1/12 pass!
        /** nested in order */ // TODO: 2018/1/12 not support for now
//        String sql = "select XM_s, keyword(LG.LGDM_s), LG.LDSJ_year_s from " + index + "." + type + " order by LG.LDSJ_year_s limit 10"; // TODO: 2018/1/12 pass!

        String sql = "select XM_s, LG.LGDM_s from " + index + "." + type + " where LG.LGDM_s = '3301032076'";
        JsonObject json = SqlExplainer.explain(sql);
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        System.out.println("============== explainResult =======================");
        System.out.println(json);
        requestBuilder = new SearchRequestBuilderVisitor(requestBuilder, json).get();
        requestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

        System.out.println("============== requestBuilder =======================");
        System.out.println(requestBuilder);
        System.out.println("============== response =============================");
        SearchResponse response = requestBuilder.get();
        System.out.println("found total: " + response.getHits().getTotalHits());
        response.getHits().forEach(hit -> System.out.println(hit.getSourceAsString()));

        System.out.println(response);
    }

    @Test
    public void g1() {
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        requestBuilder.setIndices(index);
        requestBuilder.setTypes(type);
//        QueryBuilder query = QueryBuilders.nestedQuery("LG", QueryBuilders.termQuery("LG.LGDM_s", "3301060608"), ScoreMode.Avg);
//        requestBuilder.setQuery(query);
//        AggregationBuilder aggBuilder = AggregationBuilders.count("MZ_s_count").field("MZ_s");
//        AggregationBuilder aggBuilder = AggregationBuilders.count("XB_s_count").field("XB_s.keyword");
        AggregationBuilder aggBuilder;

        AggregationBuilder a1 = AggregationBuilders.count("COUNT(LG.SSXQ_CODE6_s)").field("LG.SSXQ_CODE6_s");
        AggregationBuilder a2 = AggregationBuilders.nested("nested_1", "LG").subAggregation(a1);
        AggregationBuilder a3 = AggregationBuilders.nested("nested_3", "LG").subAggregation(
                AggregationBuilders.terms("term_1").field("LG.SSXQ_CODE6_s").subAggregation(a2)
        );
        AggregationBuilder a4 = AggregationBuilders.nested("nested_4", "LG").subAggregation(
                AggregationBuilders.terms("term_2").field("LG.RZSJ_year_s").subAggregation(a3)
        );
        aggBuilder = a4;
        requestBuilder.addAggregation(aggBuilder);
        requestBuilder.setFetchSource(new String[] {"MZ_s"}, new String[]{});
        requestBuilder.setSize(1);
        System.out.println(requestBuilder);
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++");
        SearchResponse response = requestBuilder.get();
        System.out.println("total: " + response.getHits().getTotalHits());
        System.out.println(response);
    }

    @Test
    public void n2() {
        /** nested in group by */
//        String sql = "select count(MZ_s) from " + index + "." + type + " group by MZ_s";
        String sql = "select count(MZ_s) from " + index + "." + type + " group by XB_s, MZ_s";  /** for agg test String sql = "select XM_s from " + index + "." + type + " where XB_s = '1' and MZ_s = '08'"; */

//        String sql = "select count(LG.LGDM_s.keyword) from " + index + "." + type + " group by LG.LGDM_s"; // String sql = "select XM_s, LG.LGDM_s from " + index + "." + type + " where LG.LGDM_s = '3301032076'"; // 得到两条结果，值为3301032076共55条

//        String sql = "select count(LG.SSXQ_CODE6_s) from " + index + "." + type + " group by LG.RZSJ_year_s, LG.SSXQ_CODE6_s"; // TODO: 2018/1/12 pass!
        JsonObject json = SqlExplainer.explain(sql);
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        System.out.println("============== explainResult =======================");
        System.out.println(json);
        requestBuilder = new SearchRequestBuilderVisitor(requestBuilder, json).get();
        requestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

        System.out.println("============== requestBuilder =======================");
        System.out.println(requestBuilder);
        System.out.println("============== response =============================");
        SearchResponse response = requestBuilder.get();
        System.out.println("found total: " + response.getHits().getTotalHits());
        System.out.println(response);
    }
}
