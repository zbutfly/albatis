package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkOutput;

import net.butfly.albacore.io.URISpec;

public class SparkKafkaOutput extends SparkOutput {
	private static final long serialVersionUID = 9003837433163351306L;
	private static final String format = "kafka";
	final static String schema = "kafka";

	SparkKafkaOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected Map<String, String> options(URISpec uriSpec) {
		Map<String, String> options = new HashMap<>();
		options.put("kafka.bootstrap.servers", uriSpec.getHost());
		options.put("subscribe", uriSpec.getFile());
		options.put("startingOffsets", "earliest");
		return options;
	}

	@Override
	public void enqueue(Dataset<Row> dataset) {
		// TODO Auto-generated method stub
	}
}
