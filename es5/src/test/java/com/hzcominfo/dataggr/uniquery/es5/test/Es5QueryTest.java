package com.hzcominfo.dataggr.uniquery.es5.test;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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

        // TODO: 2017/11/29 other settings
//        requestBuilder.storedFields("LOCATION");

        SearchResponse response = requestBuilder.get();
        /*System.out.println("total: " + response.getHits().getTotalHits());
        response.getHits().forEach(hit -> {
            System.out.println(hit.getFields());
        });*/
        System.out.println(response);
    }
}
