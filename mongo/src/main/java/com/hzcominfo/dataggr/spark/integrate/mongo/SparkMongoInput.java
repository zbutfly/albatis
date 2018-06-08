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
	private static final String partitioner = "MongoSamplePartitioner";
	final static String schema = "mongodb";

	protected SparkMongoInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	public Dataset<Row> load() {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(options(targetUri));
		return MongoSpark.load(jsc, readConfig).toDF();
	}

	@Override
	protected Map<String, String> options(URISpec uriSpec) {
		String file = uriSpec.getFile();
		String[] path = uriSpec.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb uriSpec is incorrect");
		String database = path[0];
		String collection = file;
		String uri = uriSpec.getScheme() + "://" + uriSpec.getAuthority() + "/" + database;

		Map<String, String> options = new HashMap<String, String>();
		options.put("partitioner", partitioner);
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", collection);
		return options;
	}
}
