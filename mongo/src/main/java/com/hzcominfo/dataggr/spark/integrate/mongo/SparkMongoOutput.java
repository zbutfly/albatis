package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.hzcominfo.dataggr.spark.io.SparkOutput;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

import net.butfly.albacore.io.URISpec;

public class SparkMongoOutput extends SparkOutput {
	private static final long serialVersionUID = -887072515139730517L;
	private static final String writeconcern = "majority";
	private final WriteConfig writeConfig;
	final static String schema = "mongodb";

	public SparkMongoOutput(SparkSession spark, URISpec destUri) {
		super(spark, destUri);
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		Map<String, String> options = options(destUri);
		this.writeConfig = WriteConfig.create(jsc).withOptions(options);
	}

	@Override
	protected Map<String, String> options(URISpec uriSpec) {
		String file = uriSpec.getFile();
		String[] path = uriSpec.getPaths();
		if (path.length != 1)
			throw new RuntimeException("Mongodb uriSpec is incorrect");
		String database = path[0];
		String collection = file;
		String uri = uriSpec.getScheme() + "://" + uriSpec.getAuthority() + "/" + database;

		Map<String, String> options = new HashMap<String, String>();
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", collection);
		options.put("writeConcern.w", writeconcern);
		return options;
	}

	@Override
	public void enqueue(Dataset<Row> dataset) {
		if (dataset.isStreaming()) {
			MongoStreamWriter writer = new MongoStreamWriter(options(targetUri));
			try {
				dataset.writeStream().foreach(writer).start().awaitTermination();
			} catch (StreamingQueryException e) {
				throw new RuntimeException("streaming query: " + e);
			};
		} else
			MongoSpark.save(dataset, writeConfig);
	}
}
