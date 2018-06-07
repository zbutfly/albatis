package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.integrate.Adapter;

import net.butfly.albacore.io.URISpec;

public class KafkaAdapter extends Adapter {
	private static final long serialVersionUID = 9003837433163351306L;
	private static final String format = "kafka";
	final static String schema = "bootstrap";
	
	public KafkaAdapter() {}
	
	@Override
	public Dataset<Row> read(URISpec uriSpec, SparkSession spark) {
		return spark.readStream().format(format).options(options(uriSpec)).load();
	}

	private Map<String, String> options(URISpec uriSpec) {
		Map<String, String> options = new HashMap<>();
		options.put("kafka.bootstrap.servers", uriSpec.getHost());
		options.put("subscribe", uriSpec.getFile());
		options.put("startingOffsets", "earliest");
		return options;
	}

	@Override
	public void write(URISpec uriSpec, SparkSession spark, Dataset<Row> dataset) {
		// TODO Auto-generated method stub
		
	}
}
