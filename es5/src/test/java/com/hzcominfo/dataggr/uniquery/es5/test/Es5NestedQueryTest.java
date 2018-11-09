package com.hzcominfo.dataggr.uniquery.es5.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVisitor;

public class Es5NestedQueryTest {

    private static String index = "sell_index", type = "sell_type";

    private TransportClient client;
    private IndicesAdminClient indexAdminClient;

    @SuppressWarnings("resource")
	@Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "pseudo-elasticsearch")
//                .put("client.transport.sniff", true)
//                .put("client.transport.ignore_cluster_name", true)
                .build();
        client = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.16.16.232"), 9300));
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.27.232"), 9300));
        indexAdminClient = client.admin().indices();
    }

    private static String nested_map_str() {
        return "{\n" +
                "  \"properties\" : {\n" +
                "    \"name\" : {\n" +
                "      \"type\" : \"text\",\n" +
                "      \"fields\" : {\n" +
                "        \"keyword\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"records\" : {\n" +
                "      \"type\" : \"nested\",\n" +
                "      \"properties\" : {\n" +
                "        \"years\" : {\n" +
                "          \"type\" : \"nested\",\n" +
                "          \"properties\" : {\n" +
                "            \"months\" : {\n" +
                "              \"type\" : \"nested\",\n" +
                "              \"properties\" : {\n" +
                "                \"days\" : {\n" +
                "                  \"type\" : \"nested\",\n" +
                "                  \"properties\" : {\n" +
                "                    \"SJD\" : {\n" +
                "                      \"type\" : \"text\",\n" +
                "                      \"fields\" : {\n" +
                "                        \"keyword\": {\n" +
                "                          \"type\": \"keyword\"\n" +
                "                        }\n" +
                "                      }\n" +
                "                    },\n" +
                "                    \"total\" : {\n" +
                "                      \"type\" : \"integer\"\n" +
                "                    }\n" +
                "                  }\n" +
                "                },\n" +
                "                \"day_total\" : {\n" +
                "                  \"type\" : \"integer\"\n" +
                "                }\n" +
                "              }\n" +
                "            },\n" +
                "            \"month_total\" : {\n" +
                "              \"type\" : \"integer\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"year_total\" : {\n" +
                "          \"type\" : \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }

    private static String nested_map_str_old() {
        return "{\n" +
                "  \"properties\" : {\n" +
                "    \"name\" : {\n" +
                "      \"type\" : \"string\"\n" +
                "    },\n" +
                "    \"records\" : {\n" +
                "      \"type\" : \"nested\",\n" +
                "      \"properties\" : {\n" +
                "        \"years\" : {\n" +
                "          \"type\" : \"nested\",\n" +
                "          \"properties\" : {\n" +
                "            \"months\" : {\n" +
                "              \"type\" : \"nested\",\n" +
                "              \"properties\" : {\n" +
                "                \"days\" : {\n" +
                "                  \"type\" : \"nested\",\n" +
                "                  \"properties\" : {\n" +
                "                    \"SJD\" : {\n" +
                "                      \"type\" : \"string\"\n" +
                "                    },\n" +
                "                    \"total\" : {\n" +
                "                      \"type\" : \"integer\"\n" +
                "                    }\n" +
                "                  }\n" +
                "                },\n" +
                "                \"day_total\" : {\n" +
                "                  \"type\" : \"integer\"\n" +
                "                }\n" +
                "              }\n" +
                "            },\n" +
                "            \"month_total\" : {\n" +
                "              \"type\" : \"integer\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"year_total\" : {\n" +
                "          \"type\" : \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }

    @Test
    public void create_nested_index() throws IOException {

        if (indexAdminClient.exists(new IndicesExistsRequest(index)).actionGet().isExists()) {
            indexAdminClient.prepareDelete(index).get();
        }
        indexAdminClient.prepareCreate(index).get();
        PutMappingRequestBuilder mappingRequestBuilder = indexAdminClient.preparePutMapping(index);
        mappingRequestBuilder.setType(type);

        mappingRequestBuilder.setSource(nested_map_str(), XContentType.JSON);
        mappingRequestBuilder.get();
    }


    @Test
    public void put_nested_data() {
/*
        curl -X POST 172.16.27.232:9200/sell_index/sell_type -d '{"name": "aa", "records": [{"years": [{"months": [{"days": [{"SJD":"SW", "total": 100},{"SJD":"XW", "total": 120}], "day_total": 220}, {"days": [{"SJD":"SW", "total": 75},{"SJD":"XW", "total": 33}], "day_total": 108}], "month_total":888}, {"months": [{"days": [{"SJD":"SW", "total": 100},{"SJD":"XW", "total": 120}], "day_total": 220}, {"days": [{"SJD":"SW", "total": 543},{"SJD":"XW", "total": 32}], "day_total": 34}], "month_total":342}], "year_total": 999}, {"years": [{"months": [{"days": [{"SJD":"SW", "total": 8484},{"SJD":"XW", "total": 963}], "day_total": 963}, {"days": [{"SJD":"SW", "total": 786},{"SJD":"XW", "total": 748}], "day_total": 516}], "month_total":879}, {"months": [{"days": [{"SJD":"SW", "total": 639},{"SJD":"XW", "total": 845}], "day_total": 264}, {"days": [{"SJD":"SW", "total": 12},{"SJD":"XW", "total": 987}], "day_total": 787}], "month_total":765}], "year_total": 5634}]}'
        curl -X POST 172.16.27.232:9200/sell_index/sell_type -d '{"name": "bb", "records": [{"years": [{"months": [{"days": [{"SJD":"SW", "total": 9999},{"SJD":"XW", "total": 2356}], "day_total": 3424}, {"days": [{"SJD":"SW", "total": 213},{"SJD":"XW", "total": 786}], "day_total": 987}], "month_total":1232}, {"months": [{"days": [{"SJD":"SW", "total": 645},{"SJD":"XW", "total": 543}], "day_total": 654}, {"days": [{"SJD":"SW", "total": 1213},{"SJD":"XW", "total": 312}], "day_total": 334}], "month_total":3432}], "year_total": 9299}, {"years": [{"months": [{"days": [{"SJD":"SW", "total": 433},{"SJD":"XW", "total": 67}], "day_total": 2366}, {"days": [{"SJD":"SW", "total": 8716},{"SJD":"XW", "total": 765}], "day_total": 12331}], "month_total":7657}, {"months": [{"days": [{"SJD":"SW", "total": 11},{"SJD":"XW", "total": 22}], "day_total": 33}, {"days": [{"SJD":"SW", "total": 2},{"SJD":"XW", "total": 7}], "day_total": 87}], "month_total":75}], "year_total": 34}]}'
        curl -X POST 172.16.27.232:9200/sell_index/sell_type -d '{"name": "cc", "records": [{"years": [{"months": [{"days": [{"SJD":"SW", "total": 8888},{"SJD":"XW", "total": 1245}], "day_total": 45434}, {"days": [{"SJD":"SW", "total": 435},{"SJD":"XW", "total": 322}], "day_total": 785}], "month_total":355}, {"months": [{"days": [{"SJD":"SW", "total": 645},{"SJD":"XW", "total": 543}], "day_total": 654}, {"days": [{"SJD":"SW", "total": 1213},{"SJD":"XW", "total": 312}], "day_total": 334}], "month_total":3432}], "year_total": 9299}, {"years": [{"months": [{"days": [{"SJD":"SW", "total": 433},{"SJD":"XW", "total": 67}], "day_total": 2366}, {"days": [{"SJD":"SW", "total": 8716},{"SJD":"XW", "total": 765}], "day_total": 12331}], "month_total":7657}, {"months": [{"days": [{"SJD":"SW", "total": 11},{"SJD":"XW", "total": 22}], "day_total": 33}, {"days": [{"SJD":"SW", "total": 2},{"SJD":"XW", "total": 7}], "day_total": 87}], "month_total":75}], "year_total": 34}]}'
        curl -X POST 172.16.27.232:9200/sell_index/sell_type -d '{"name": "dd", "records": [{"years": [{"months": [{"days": [{"SJD":"ZW", "total": 435},{"SJD":"WS", "total": 654}], "day_total": 23}, {"days": [{"SJD":"ZW", "total": 768},{"SJD":"WS", "total": 987}], "day_total": 82}], "month_total":26}, {"months": [{"days": [{"SJD":"ZW", "total": 51},{"SJD":"WS", "total": 762}], "day_total": 923}, {"days": [{"SJD":"ZW", "total": 9342},{"SJD":"WS", "total": 865}], "day_total": 540}], "month_total":103}], "year_total": 703}, {"years": [{"months": [{"days": [{"SJD":"ZW", "total": 973},{"SJD":"WS", "total": 16}], "day_total": 824}, {"days": [{"SJD":"ZW", "total": 4345},{"SJD":"WS", "total": 86}], "day_total": 145}], "month_total":987}, {"months": [{"days": [{"SJD":"ZW", "total": 1090},{"SJD":"WS", "total": 22}], "day_total": 33}, {"days": [{"SJD":"ZW", "total": 2},{"SJD":"WS", "total": 7}], "day_total": 87}], "month_total":75}], "year_total": 34}]}'
*/
    }

    @Test
    public void nested2_query() {
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        requestBuilder.setIndices(index);
        requestBuilder.setTypes(type);

        // TODO: 2017/12/7 in condition
//        QueryBuilder query = QueryBuilders.matchAllQuery();
//        QueryBuilder query = QueryBuilders.nestedQuery("records.years.months", QueryBuilders.matchQuery("records.years.months.day_total", 220), ScoreMode.None);
        QueryBuilder query = QueryBuilders.nestedQuery("records.years.months.days", QueryBuilders.termsQuery("SJD", "SW"), ScoreMode.Avg);
//        QueryBuilder query = QueryBuilders.nestedQuery("records.years.months", QueryBuilders.rangeQuery("records.years.months.day_total").from(1), ScoreMode.None);
        requestBuilder.setQuery(query);
        // TODO: 2017/12/7 in field
//        requestBuilder.setFetchSource(new String[] {"records.years.months.day_total"/*, "records.years.month_total"*/}, new String[]{});

        // TODO: 2017/12/7 in sort
//        FieldSortBuilder sortBuilder = SortBuilders.fieldSort("records.years.months.day_total")
//                .setNestedPath("records.years.months").order(SortOrder.ASC).sortMode(SortMode.MAX)/*.setNestedFilter()*/;
//        requestBuilder.addSort(sortBuilder);

        // TODO: 2017/12/8 in agg
       /* AggregationBuilder aggregationBuilder = AggregationBuilders.nested("records.years.months" + "_aggrs", "records.years.months")
                *//*.subAggregation(AggregationBuilders.sum("records.years.months.day_total" + "_aggrs").field("records.years.months.day_total"))*//*;
//        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("name_agg").field("name.keyword");
//        AggregationBuilders.nested("agg_name", "records.years.months.day_total").
        requestBuilder.addAggregation(aggregationBuilder);*/

//        requestBuilder.setSize(0);

        System.out.println(requestBuilder);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
        SearchResponse response = requestBuilder.get();
//        System.out.println(response);
        response.getHits().forEach(hit -> System.out.println(hit.getSourceAsString()));


    }

    @Test
    public void query() {
        String sql = "select * from " + index + "." + type + "";
//        String sql = "select _id, name, records.years.month_total from " + index + "." + type + " where records.years.months.days.total > 0";
//        String sql = "select _id, name, records.years.month_total from " + index + "." + type + " where records.years.month_total > 888";
//        String sql = "select _id, name, records.years.month_total from " + index + "." + type;
//        String sql = "select _id, keyword(name), records.years.month_total from " + index + "." + type + " where keyword(records.years.months.days.SJD) = 'SW'";
//        String sql = "select _id, name, records.years.month_total from " + index + "." + type + " where records.years.month_total > 900";
//        String sql = "select _id, name, records.years.month_total from " + index + "." + type + " where records.years.month_total > 900 order by records.years.month_total desc";
//        String sql = "select _id, keyword(name), records.years.month_total from " + index + "." + type + " where keyword(records.years.months.days.SJD) = 'SW' order by keyword(records.years.months.days.SJD) desc";
//        String sql = "select _id, keyword(name), records.years.month_total from " + index + "." + type + " where keyword(records.years.months.days.SJD) = 'SW' order by keyword(name) desc";
//        String sql = "select _id, keyword(name), records.years.month_total from " + index + "." + type + " where keyword(records.years.months.days.SJD) in ('SW', 'XW', 'ZW') order by keyword(name) desc";
//        String sql = "select * from " + index + "." + type + " where keyword(records.years.months.days.SJD) in ('SW', 'XW', 'ZW') order by keyword(name) desc";

        System.out.println(sql);
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
        System.out.println(response);
    }

    @Test
    public void t1() {
        SearchRequestBuilder requestBuilder = client.prepareSearch("");
        requestBuilder.setIndices(index);
        requestBuilder.setTypes(type);

//        QueryBuilder query = QueryBuilders.matchAllQuery();
//        QueryBuilder query = QueryBuilders.nestedQuery("records.years.months.days", QueryBuilders.termQuery("records.years.months.days.SJD", "SW"), ScoreMode.Avg);
//        QueryBuilder query = QueryBuilders.nestedQuery("records", QueryBuilders.termQuery("records.year_total", 9299), ScoreMode.Max);
        QueryBuilder query = QueryBuilders.nestedQuery("records.years.months.days", QueryBuilders.termQuery("records.years.months.days.SJD.keyword", "SW"), ScoreMode.None);
//        QueryBuilder query = QueryBuilders.nestedQuery("records.years.months.days", QueryBuilders.termQuery("records.years.months.days.total", 120), ScoreMode.None);
        requestBuilder.setQuery(query);
//        requestBuilder.setFetchSource(new String[] {"name.keyword"}, new String[]{});  // TODO: 2018/1/11 field不用带 .keyword ?
//        requestBuilder.setFetchSource(new String[] {"name", }, new String[]{});

        System.out.println(requestBuilder);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
        SearchResponse response = requestBuilder.get();
        System.out.println(response);
    }

    @Test
    public void m1() {
        System.out.println(nested_map_str());
    }

}
