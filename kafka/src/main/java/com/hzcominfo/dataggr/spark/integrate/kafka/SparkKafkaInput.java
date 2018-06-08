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
	private static final String format = "kafka";
	final static String schema = "kafka";

	protected SparkKafkaInput(SparkSession spark, URISpec targetUri) {
		this(spark, targetUri, new String[] {});
	}

	SparkKafkaInput(SparkSession spark, URISpec targetUri, String... fields) {
		super(spark, targetUri, fields);
	}

	@Override
	public Dataset<Row> dequeue() {
		Dataset<Row> dataset = spark.readStream().format(format).options(options(targetUri)).load();
		if (fields != null && fields.length > 0) 
			return super.map(dataset);
		else
			return dataset;
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
	protected Row selectItems(Row r) {
		return super.defaultSelectItems(r);
	}
}
