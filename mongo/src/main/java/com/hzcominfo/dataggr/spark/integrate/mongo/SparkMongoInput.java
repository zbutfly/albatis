package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.util.RowConsumer;
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
	public StreamingQuery read(RowConsumer using) {
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(options());
		MongoSpark.load(jsc, readConfig).toDF().foreach(using::accept);
		return null;
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
