package com.hzcominfo.dataggr.uniquery.es5.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.es5.SearchRequestBuilderVisitor;

public class Es5QueryTest {
	TransportClient client;

	@SuppressWarnings("resource")
	@Before
	public void init() throws UnknownHostException {
		/*
		 * Settings settings = Settings.builder() .put("cluster.name", "cidev") //
		 * .put("client.transport.sniff", true) //
		 * .put("client.transport.ignore_cluster_name", true) .build(); client = new
		 * PreBuiltTransportClient(settings) .addTransportAddress(new
		 * InetSocketTransportAddress(InetAddress.getByName("172.16.17.11"), 39300))
		 * .addTransportAddress(new
		 * InetSocketTransportAddress(InetAddress.getByName("172.16.17.12"), 39300))
		 * .addTransportAddress(new
		 * InetSocketTransportAddress(InetAddress.getByName("172.16.17.13"), 39300));
		 */
		Settings settings = Settings.builder().put("cluster.name", "hzcominfo")
				// .put("client.transport.sniff", true)
				// .put("client.transport.ignore_cluster_name", true)
				.build();
		client = new PreBuiltTransportClient(settings)
				// .addTransportAddress(new
				// InetSocketTransportAddress(InetAddress.getByName("172.16.16.232"), 9300));
				.addTransportAddress(new TransportAddress(InetAddress.getByName("172.30.10.31"), 39300));
	}

	@Test
	public void query() {
		// String sql = "select * from uniquery.sell";
		// String sql = "select name, sell from uniquery.sell";
		// String sql = "select * from uniquery.sell order by name.keyword desc, sell
		// desc";
		// String sql = "select * from uniquery.sell order by name.keyword desc, sell
		// desc limit 4 offset 2";
		// String sql = "select * from uniquery.sell limit 4";
		// String sql = "select * from uniquery.sell offset 4";
		// String sql = "select * from uniquery.sell limit 4 offset 2";

		// String sql = "select * from uniquery.sell where name.keyword='xx'";
		// String sql = "select * from uniquery.sell where name.keyword='xx' and sell =
		// 200";
		// String sql = "select * from uniquery.sell where name.keyword='xx' or
		// name.keyword = 'yy'";
		// String sql = "select * from uniquery.sell where (name.keyword='xx' and sell =
		// '200') or (name.keyword = 'yy' and sell = '250')";
		// String sql = "select * from uniquery.sell where name.keyword > 'xx'";
		// String sql = "select * from uniquery.sell where name.keyword >= 'yy'";
		// String sql = "select * from uniquery.sell where sell <= 100";
		// String sql = "select * from uniquery.sell where name.keyword < 'yy'";
		// String sql = "select * from uniquery.sell where name.keyword <= 'yy'";
		// String sql = "select * from uniquery.sell where name.keyword <> 'yy'"; //
		// where name != 'yy'
		// String sql = "select * from uniquery.sell where name.keyword like 'y%'";
		// String sql = "select * from uniquery.sell where name.keyword like '%'";
		// String sql = "select * from uniquery.sell where name.keyword not like 'y%'";
		// String sql = "select * from uniquery.sell where remark.keyword is not null";
		// String sql = "select * from uniquery.sell where remark.keyword is null";
		// String sql = "select * from uniquery.sell where name.keyword in ('xx',
		// 'yy')";
		// String sql = "select * from uniquery.sell where name.keyword not in ('xx',
		// 'yy')";
		// String sql = "select * from uniquery.sell where name.keyword between 'xx' and
		// 'yy'";
		// String sql = "select * from uniquery.sell where name.keyword not between 'xx'
		// and 'yy'";

		// String sql = "select name, years, count(sell) as s from uniquery.sell group
		// by name.keyword, years"; // TODO: 2017/12/4 AS for agg fun has not support
		// well
		// String sql = "SELECT * FROM test_hzwa.WA_SOURCE_FJ_1001_TEST_JZ WHERE
		// MAC_s.keyword = 'E0-3E-44-04-00-00' AND CAPTURE_TIME_s.keyword BETWEEN
		// '1492012800' AND '1523635199' ORDER BY CAPTURE_TIME_s.keyword ASC\n";
		String sql = "SELECT * FROM test_hzwa.WA_SOURCE_FJ_1001_TEST_JZ WHERE MAC_s.keyword = 'E0-3E-44-04-00-00' ORDER BY CAPTURE_TIME_s.keyword ASC";

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
