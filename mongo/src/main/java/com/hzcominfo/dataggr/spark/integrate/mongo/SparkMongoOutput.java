package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkOutput;

import net.butfly.albacore.io.URISpec;

public class SparkMongoOutput extends SparkOutput {
	private static final long serialVersionUID = -887072515139730517L;
	private static final String partitioner = "MongoSamplePartitioner";
	final static String schema = "mongodb";

	public SparkMongoOutput(SparkSession spark, URISpec destUri) {
		super(spark, destUri);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected Map<String, String> options(URISpec uriSpec) {
		String file = uriSpec.getFile();
		String database = null, collection = null;
		String uri = uriSpec.toString();
		if (!file.contains(".")) throw new RuntimeException("Mongodb uriSpec error: " + uri);
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

	@Override
	public void enqueue(Dataset<Row> dataset) {
		// TODO: use targetUri, spark, to write dataset
	}
}
