package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkOutput;

import net.butfly.albacore.io.URISpec;

public class SparkKafkaOutput extends SparkOutput {

	private static final long serialVersionUID = 9003837433163351306L;

	public SparkKafkaOutput() {
		super();
	}

	SparkKafkaOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected Map<String, String> options() {
		Map<String, String> options = new HashMap<>();
		options.put("kafka.bootstrap.servers", targetUri.getHost());
		options.put("subscribe", targetUri.getFile());
		options.put("startingOffsets", "earliest");
		return options;
	}

	@Override
	public void write(Row row) {
		// TODO Auto-generated method stub
	}

	@Override
	protected String schema() {
		return "kafka";
	}
}
