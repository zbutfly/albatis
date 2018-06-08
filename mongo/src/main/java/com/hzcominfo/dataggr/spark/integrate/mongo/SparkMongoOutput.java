package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkOutput;
import com.hzcominfo.dataggr.spark.util.FuncUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class SparkMongoOutput extends SparkOutput {
	private static final Logger logger = Logger.getLogger(SparkMongoOutput.class);
	private static final long serialVersionUID = -887072515139730517L;
	private static final String writeconcern = "majority";
	// private WriteConfig writeConfig;
	private DBCollection coll;
	private MongoClient mongoClient;

	public SparkMongoOutput() {
		super();
	}

	public SparkMongoOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		Map<String, String> opts = options();
		// writeConfig = WriteConfig.create(jsc).withOptions(opts);
		mongoClient = new MongoClient(new MongoClientURI(opts.get("uri")));
		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB(opts.get("database"));
		coll = db.getCollection(opts.get("collection"));
	}

	@Override
	public void close() {
		super.close();
		mongoClient.close();
	}

	@Override
	protected Map<String, String> options() {
		String file = targetUri.getFile();
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb URI is incorrect");
		String database = path[0];
		String collection = file;
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		Map<String, String> options = new HashMap<String, String>();
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", collection);
		options.put("writeConcern.w", writeconcern);
		return options;
	}

	@Override
	public void write(Row row) {
		coll.insert(new BasicDBObject(FuncUtil.rowMap(row)));
		logger.info("inserted: " + row.toString());
		// MongoSpark.save(ds, writeConfig );
	}

	@Override
	protected String schema() {
		return "mongodb";
	}
}
