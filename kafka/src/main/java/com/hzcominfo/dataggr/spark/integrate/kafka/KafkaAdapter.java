package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.HashMap;
import java.util.Map;

import com.hzcominfo.dataggr.spark.integrate.Adapter;
import com.hzcominfo.dataggr.spark.integrate.ReadConfig;

import net.butfly.albacore.io.URISpec;

public class KafkaAdapter extends Adapter {
	final static String schema = "bootstrap";
	public KafkaAdapter() {}
	
	@Override
	public ReadConfig getConfig(URISpec uri) {
		Map<String, String> options = new HashMap<>();
		options.put("kafka.bootstrap.servers", uri.getHost());
		options.put("subscribe", uri.getFile());
		options.put("startingOffsets", "earliest");
		return new ReadConfig("kafka", options, true);
	}
}
