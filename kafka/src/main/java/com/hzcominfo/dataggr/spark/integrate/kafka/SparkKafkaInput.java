package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;

import net.butfly.albacore.io.URISpec;

public class SparkKafkaInput extends SparkInput {
	private static final long serialVersionUID = 9003837433163351306L;
	final static String schema = "kafka";

	public SparkKafkaInput() {
		super();
	}

	protected SparkKafkaInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	
	@Override
	public Dataset<Row> read() {
		return spark.readStream().format(format()).options(options()).load();
	}

	@Override
	protected String format() {
		return "kafka";
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
	protected String schema() {
		return "kafka";
	}
}
