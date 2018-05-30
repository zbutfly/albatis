package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.integrate.Adapter;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import net.butfly.albacore.io.URISpec;

public class MongoAdapter extends Adapter {
	private static final long serialVersionUID = 9003837433163351306L;
	private static final String partitioner = "MongoSamplePartitioner";
	final static String schema = "mongodb";

	public MongoAdapter() {
	}

	@Override
	public Dataset<Row> read(URISpec uriSpec, SparkSession spark) {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(options(uriSpec));
		return MongoSpark.load(jsc, readConfig).toDF();
	}

	private Map<String, String> options(URISpec uriSpec) {
		String file = uriSpec.getFile();
		String database = null, collection = null;
		String uri = uriSpec.toString();
		if (!file.contains("."))
			throw new RuntimeException("Mongodb uriSpec error: " + uri);
		String[] split = file.split("\\.");
		database = split[0];
		collection = split[1];
		uri = uri.replace("." + collection, "");

		Map<String, String> options = new HashMap<String, String>();
		options.put("partitioner", partitioner);
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", collection);
		return options;
	}
}
