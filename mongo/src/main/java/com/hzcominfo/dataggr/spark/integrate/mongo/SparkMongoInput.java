package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import net.butfly.albacore.io.URISpec;

public class SparkMongoInput extends SparkInput {
	private static final long serialVersionUID = 2110132305482403155L;

	protected SparkMongoInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	public SparkMongoInput() {
		super();
	}

	@Override
	public Dataset<Row> read() {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(options());
		return MongoSpark.load(jsc, readConfig).toDF();
	}

	@Override
	protected Map<String, String> options() {
		String file = targetUri.getFile();
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb uriSpec is incorrect");
		String database = path[0];
		String collection = file;
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		Map<String, String> options = new HashMap<String, String>();
		options.put("partitioner", "MongoSamplePartitioner");
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", collection);
		return options;
	}

	@Override
	protected String schema() {
		return "mongodb";
	}
}
